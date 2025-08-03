use rustler::{Atom, Binary, Env, Term, ResourceArc, OwnedBinary, NifResult, Error, Encoder, resource};
use sled::{Db, Config, Mode, IVec};
use std::panic::{RefUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};

mod atoms;

// Configuration options for opening a database
#[derive(Debug, Clone)]
pub struct DbOptions {
    cache_capacity: Option<u64>,
    mode: Option<String>,
    compression_factor: Option<i32>,
    flush_every_ms: Option<u64>,
}

pub struct DbHandle {
    db: Db,
    is_open: Arc<RwLock<bool>>,
}

// Manually implement RefUnwindSafe for DbHandle 
// This is safe because sled::Db is designed to be thread-safe
impl RefUnwindSafe for DbHandle {}


impl Drop for DbHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl DbHandle {
    fn new(path: &str, options: DbOptions) -> Result<Self, sled::Error> {
        let mut config = Config::new().path(path);
        
        if let Some(cache_capacity) = options.cache_capacity {
            config = config.cache_capacity(cache_capacity);
        }
        
        if let Some(mode_str) = options.mode {
            let mode = match mode_str.as_str() {
                "fast" => Mode::HighThroughput,
                "safe" => Mode::LowSpace,
                _ => Mode::HighThroughput,
            };
            config = config.mode(mode);
        }
        
        if let Some(factor) = options.compression_factor {
            config = config.compression_factor(factor);
        }
        
        if let Some(flush_ms) = options.flush_every_ms {
            config = config.flush_every_ms(Some(flush_ms));
        }
        
        let db = config.open()?;
        Ok(DbHandle { 
            db,
            is_open: Arc::new(RwLock::new(true)),
        })
    }
    
    fn close(&self) -> Result<(), sled::Error> {
        let mut is_open = self.is_open.write().unwrap();
        if *is_open {
            *is_open = false;
            self.db.flush()?;
        }
        Ok(())
    }
    
    fn is_open(&self) -> bool {
        *self.is_open.read().unwrap()
    }
}

// Helper function to safely execute operations and catch panics
fn safe_execute<F, R>(f: F) -> NifResult<R>
where
    F: FnOnce() -> Result<R, sled::Error> + std::panic::UnwindSafe,
{
    match catch_unwind(|| f()) {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(e)) => Err(convert_sled_error(e)),
        Err(_) => Err(Error::RaiseAtom("panic_in_nif")),
    }
}

// Convert sled errors to Erlang terms
fn convert_sled_error(error: sled::Error) -> Error {
    match error {
        sled::Error::Io(io_error) => {
            match io_error.kind() {
                std::io::ErrorKind::NotFound => Error::RaiseTerm(Box::new((atoms::error(), atoms::not_found()))),
                std::io::ErrorKind::PermissionDenied => Error::RaiseTerm(Box::new((atoms::error(), atoms::permission_denied()))),
                std::io::ErrorKind::OutOfMemory => Error::RaiseTerm(Box::new((atoms::error(), atoms::out_of_memory()))),
                _ => Error::RaiseTerm(Box::new((atoms::error(), atoms::io_error()))),
            }
        }
        sled::Error::Corruption { .. } => Error::RaiseTerm(Box::new((atoms::error(), atoms::corruption()))),
        sled::Error::ReportableBug(_) => Error::RaiseTerm(Box::new((atoms::error(), atoms::database_error()))),
        sled::Error::Unsupported(msg) => {
            if msg == "cas_failed" {
                Error::RaiseTerm(Box::new((atoms::error(), atoms::cas_failed())))
            } else {
                Error::RaiseTerm(Box::new((atoms::error(), atoms::unsupported())))
            }
        },
        sled::Error::CollectionNotFound(_) => Error::RaiseTerm(Box::new((atoms::error(), atoms::collection_not_found()))),
    }
}

// Parse options from Erlang proplist
fn parse_options(_env: Env, options: Term) -> Result<DbOptions, sled::Error> {
    let mut db_options = DbOptions {
        cache_capacity: None,
        mode: None,
        compression_factor: None,
        flush_every_ms: None,
    };

    if let Ok(list) = options.decode::<Vec<(String, Term)>>() {
        for (key, value) in list {
            match key.as_str() {
                "cache_capacity" => {
                    if let Ok(val) = value.decode::<u64>() {
                        db_options.cache_capacity = Some(val);
                    }
                }
                "mode" => {
                    if let Ok(val) = value.decode::<String>() {
                        db_options.mode = Some(val);
                    }
                }
                "compression_factor" => {
                    if let Ok(val) = value.decode::<i32>() {
                        db_options.compression_factor = Some(val);
                    }
                }
                "flush_every_ms" => {
                    if let Ok(val) = value.decode::<u64>() {
                        db_options.flush_every_ms = Some(val);
                    }
                }
                _ => {} // Ignore unknown options
            }
        }
    }

    Ok(db_options)
}

// Check if database is open and return error if not
fn check_db_open(db_handle: &DbHandle) -> Result<(), sled::Error> {
    if !db_handle.is_open() {
        Err(sled::Error::Unsupported("Database is closed".to_string()))
    } else {
        Ok(())
    }
}

// Initialize the NIF
fn load(env: Env, _info: Term) -> bool {
    resource!(DbHandle, env);
    true
}

// Enhanced open function with configuration options
#[rustler::nif]
fn nif_open_db(env: Env, path: String, options: Term) -> NifResult<(Atom, ResourceArc<DbHandle>)> {
    safe_execute(|| {
        let db_options = parse_options(env, options)?;
        let handle = DbHandle::new(&path, db_options)?;
        Ok((atoms::ok(), ResourceArc::new(handle)))
    })
}

// Close the database
#[rustler::nif]
fn nif_close_db(db_resource: ResourceArc<DbHandle>) -> NifResult<Atom> {
    safe_execute(|| {
        db_resource.close()?;
        Ok(atoms::ok())
    })
}

// Put a key-value pair with proper error handling
#[rustler::nif]
fn nif_put(db_resource: ResourceArc<DbHandle>, key: Binary, value: Binary) -> NifResult<Atom> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        db_resource.db.insert(key.as_slice(), value.as_slice())?;
        Ok(atoms::ok())
    })
}

// Get a value by key with proper error handling
#[rustler::nif]
fn nif_get<'a>(env: Env<'a>, db_resource: ResourceArc<DbHandle>, key: Binary) -> NifResult<Term<'a>> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        match db_resource.db.get(key.as_slice())? {
            Some(value) => {
                let mut owned = OwnedBinary::new(value.len()).unwrap();
                owned.as_mut_slice().copy_from_slice(&value);
                let binary = Binary::from_owned(owned, env);
                Ok((atoms::ok(), binary).encode(env))
            }
            None => Ok(atoms::not_found().encode(env)),
        }
    })
}

// Delete a key with proper error handling
#[rustler::nif]
fn nif_delete(db_resource: ResourceArc<DbHandle>, key: Binary) -> NifResult<Atom> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        db_resource.db.remove(key.as_slice())?;
        Ok(atoms::ok())
    })
}

// Compare and swap operation - returns ok or {error, cas_failed}
#[rustler::nif]
fn nif_compare_and_swap<'a>(env: Env<'a>, db_resource: ResourceArc<DbHandle>, key: Binary, old_value: Term, new_value: Binary) -> NifResult<Term<'a>> {
    // Use catch_unwind for panic safety
    match catch_unwind(|| {
        // Check if database is open
        let is_open = db_resource.is_open.read().unwrap();
        if !*is_open {
            return Err(Error::RaiseTerm(Box::new((atoms::error(), atoms::database_closed()))));
        }
        drop(is_open);
        
        // Decode old value
        let old_val = if old_value.is_atom() {
            match old_value.decode::<Atom>() {
                Ok(atom) if atom == atoms::not_found() => None,
                Ok(_) => return Err(Error::RaiseTerm(Box::new((atoms::error(), atoms::badarg())))),
                Err(_) => return Err(Error::RaiseTerm(Box::new((atoms::error(), atoms::badarg())))),
            }
        } else {
            match old_value.decode::<Binary>() {
                Ok(old_binary) => Some(IVec::from(old_binary.as_slice())),
                Err(_) => return Err(Error::RaiseTerm(Box::new((atoms::error(), atoms::badarg())))),
            }
        };

        // Perform CAS operation
        match db_resource.db.compare_and_swap(key.as_slice(), old_val, Some(IVec::from(new_value.as_slice()))) {
            Ok(Ok(_)) => Ok(Ok(atoms::ok())),
            Ok(Err(_)) => Ok(Err((atoms::error(), atoms::cas_failed()))),  // Return tuple, not error
            Err(e) => Err(convert_sled_error(e)),
        }
    }) {
        Ok(Ok(Ok(atom))) => Ok(atom.encode(env)),
        Ok(Ok(Err(tuple))) => Ok(tuple.encode(env)),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(Error::RaiseAtom("panic_in_nif")),
    }
}

// List keys with prefix
#[rustler::nif]
fn nif_list<'a>(env: Env<'a>, db_resource: ResourceArc<DbHandle>, prefix: Binary) -> NifResult<(Atom, Vec<Binary<'a>>)> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        let mut keys = Vec::new();
        
        for result in db_resource.db.scan_prefix(prefix.as_slice()) {
            let (key, _) = result?;
            let mut owned = OwnedBinary::new(key.len()).unwrap();
            owned.as_mut_slice().copy_from_slice(&key);
            let binary = Binary::from_owned(owned, env);
            keys.push(binary);
        }
        
        Ok((atoms::ok(), keys))
    })
}

// List all key-value pairs with prefix (fold operation)
// Returns raw key-value pairs since we can't execute Erlang functions from Rust
#[rustler::nif]
fn nif_fold<'a>(env: Env<'a>, db_resource: ResourceArc<DbHandle>, _fun: Term, _init_acc: Term, prefix: Binary) -> NifResult<(Atom, Vec<(Binary<'a>, Binary<'a>)>)> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        let mut pairs = Vec::new();
        
        // Since we can't execute Erlang functions from Rust, we return the raw key-value pairs
        // and let Erlang do the folding with the provided function and initial accumulator
        for result in db_resource.db.scan_prefix(prefix.as_slice()) {
            let (key, value) = result?;
            
            let mut key_owned = OwnedBinary::new(key.len()).unwrap();
            key_owned.as_mut_slice().copy_from_slice(&key);
            let key_binary = Binary::from_owned(key_owned, env);
            
            let mut value_owned = OwnedBinary::new(value.len()).unwrap();
            value_owned.as_mut_slice().copy_from_slice(&value);
            let value_binary = Binary::from_owned(value_owned, env);
            
            pairs.push((key_binary, value_binary));
        }
        
        Ok((atoms::ok(), pairs))
    })
}

// Batch put operation
#[rustler::nif]
fn nif_batch_put(db_resource: ResourceArc<DbHandle>, kv_pairs: Vec<(Binary, Binary)>) -> NifResult<Atom> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        let mut batch = sled::Batch::default();
        
        for (key, value) in kv_pairs {
            batch.insert(key.as_slice(), value.as_slice());
        }
        
        db_resource.db.apply_batch(batch)?;
        Ok(atoms::ok())
    })
}

// Transaction support (placeholder - returns ok for now)
// Since we can't execute Erlang functions from Rust easily, this is a placeholder implementation
#[rustler::nif]
fn nif_transaction(db_resource: ResourceArc<DbHandle>, _fun: Term) -> NifResult<Atom> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        // For sled, we'll implement basic transaction support
        // This is a placeholder that just ensures the database is open
        // The _fun parameter is ignored since we can't execute Erlang functions from Rust
        Ok(atoms::ok())
    })
}

// Flush the database with proper error handling
#[rustler::nif]
fn nif_flush(db_resource: ResourceArc<DbHandle>) -> NifResult<Atom> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        db_resource.db.flush()?;
        Ok(atoms::ok())
    })
}

// Get database size on disk
#[rustler::nif]
fn nif_size_on_disk(db_resource: ResourceArc<DbHandle>) -> NifResult<(Atom, u64)> {
    safe_execute(|| {
        check_db_open(&db_resource)?;
        let size = db_resource.db.size_on_disk()?;
        Ok((atoms::ok(), size))
    })
}


rustler::init!(
    "bobsled",
    load = load
);