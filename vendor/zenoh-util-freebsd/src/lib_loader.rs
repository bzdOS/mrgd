// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/lib_loader.rs
// PURPOSE: Dynamic library loader with search path resolution and prefix/suffix matching.
// INTENT: Searches configured directories for libraries matching lib<name>.so/dylib/dll pattern, loads them via libloading.
// DEPENDENCIES: std (env::consts, ffi, ops, path), libloading, tracing, zenoh_core, zenoh_result, crate::LibSearchDirs
// PUBLIC_API: LibLoader, LIB_PREFIX, LIB_SUFFIX
// END_AI_HEADER

//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    env::consts::{DLL_PREFIX, DLL_SUFFIX},
    ffi::OsString,
    ops::Deref,
    path::PathBuf,
};

use libloading::Library;
use tracing::{debug, warn};
use zenoh_core::{zconfigurable, zerror};
use zenoh_result::{bail, ZResult};

use crate::LibSearchDirs;

zconfigurable! {
    /// The libraries prefix for the current platform (usually: `"lib"`)
    pub static ref LIB_PREFIX: String = DLL_PREFIX.to_string();
    /// The libraries suffix for the current platform (`".dll"` or `".so"` or `".dylib"`...)
    pub static ref LIB_SUFFIX: String = DLL_SUFFIX.to_string();
}

/// LibLoader allows search for libraries and to load them.
#[derive(Clone, Debug)]
pub struct LibLoader {
    search_paths: Option<Vec<PathBuf>>,
}

impl LibLoader {
    /// Return an empty `LibLoader`.
    // empty:start
//   purpose: Create a LibLoader with no search paths (only direct file loading works).
//   input:  none.
//   output: LibLoader with search_paths=None.
//   sideEffects: none
    pub fn empty() -> LibLoader {
        LibLoader { search_paths: None }
    }
    // empty:end

    /// Creates a new [LibLoader] with a set of paths where the libraries will be searched for.
    /// If `exe_parent_dir`is true, the parent directory of the current executable is also added
    /// to the set of paths for search.
    // new:start
//   purpose: Create a LibLoader by resolving given LibSearchDirs into concrete search paths.
//   input:  dirs - the search directories to resolve and use.
//   output: LibLoader with resolved search paths.
//   sideEffects: resolves each search dir (may read current exe path, expand shell vars, canonicalize)
    pub fn new(dirs: LibSearchDirs) -> LibLoader {
        let mut search_paths = Vec::new();

        for path in dirs.into_iter() {
            match path {
                Ok(path) => search_paths.push(path),
                Err(err) => tracing::error!("{err}"),
            }
        }

        LibLoader {
            search_paths: Some(search_paths),
        }
    }
    // new:end

    /// Return the list of search paths used by this [LibLoader]
    // search_paths:start
//   purpose: Return a reference to the search paths slice, or None if empty.
//   input:  &self.
//   output: Option<&[PathBuf]> - None if LibLoader was created with empty().
//   sideEffects: none
    pub fn search_paths(&self) -> Option<&[PathBuf]> {
        self.search_paths.as_deref()
    }
    // search_paths:end

    /// Load a library from the specified path.
    ///
    /// # Safety
    ///
    /// This function calls [libloading::Library::new()](https://docs.rs/libloading/0.7.0/libloading/struct.Library.html#method.new)
    /// which is unsafe.
    /// The library should be valid, or it might cause the undefined behavior.
    pub unsafe fn load_file(path: &str) -> ZResult<(Library, PathBuf)> {
        let path = Self::str_to_canonical_path(path)?;

        if !path.exists() {
            bail!("Library file '{}' doesn't exist", path.display())
        } else if !path.is_file() {
            bail!("Library file '{}' is not a file", path.display())
        } else {
            // SAFETY: Call unsafe `libloading::Library::new()`.
            unsafe { Ok((Library::new(path.clone())?, path)) }
        }
    }

    /// Search for library with filename: [struct@LIB_PREFIX]+`name`+[struct@LIB_SUFFIX] and load it.
    /// The result is a tuple with:
    ///    * the [Library]
    ///    * its full path
    ///
    /// # Safety
    ///
    /// This function calls [libloading::Library::new()](https://docs.rs/libloading/0.7.0/libloading/struct.Library.html#method.new)
    /// which is unsafe.
    /// The library should be valid, or it might cause the undefined behavior.
    pub unsafe fn search_and_load(&self, name: &str) -> ZResult<Option<(Library, PathBuf)>> {
        let filename = format!("{}{}{}", *LIB_PREFIX, name, *LIB_SUFFIX);
        let filename_ostr = OsString::from(&filename);
        tracing::debug!(
            "Search for library {} to load in {:?}",
            filename,
            self.search_paths
        );
        let Some(search_paths) = self.search_paths() else {
            return Ok(None);
        };
        for dir in search_paths {
            match dir.read_dir() {
                Ok(read_dir) => {
                    for entry in read_dir.flatten() {
                        if entry.file_name() == filename_ostr {
                            let path = entry.path();
                            // SAFETY: Call unsafe `libloading::Library::new()`.
                            return unsafe { Ok(Some((Library::new(path.clone())?, path))) };
                        }
                    }
                }
                Err(err) => debug!(
                    "Failed to read in directory {:?} ({}). Can't use it to search for libraries.",
                    dir, err
                ),
            }
        }
        Err(zerror!("Library file '{}' not found", filename).into())
    }

    /// Search and load all libraries with filename starting with [struct@LIB_PREFIX]+`prefix` and ending with [struct@LIB_SUFFIX].
    /// The result is a list of tuple with:
    ///    * the [Library]
    ///    * its full path
    ///    * its short name (i.e. filename stripped of prefix and suffix)
    ///
    /// # Safety
    ///
    /// This function calls [libloading::Library::new()](https://docs.rs/libloading/0.7.0/libloading/struct.Library.html#method.new)
    /// which is unsafe.
    /// The library should be valid, or it might cause the undefined behavior.
    pub unsafe fn load_all_with_prefix(
        &self,
        prefix: Option<&str>,
    ) -> Option<Vec<(Library, PathBuf, String)>> {
        let lib_prefix = format!("{}{}", *LIB_PREFIX, prefix.unwrap_or(""));
        tracing::debug!(
            "Search for libraries {}*{} to load in {:?}",
            lib_prefix,
            *LIB_SUFFIX,
            self.search_paths
        );
        let mut result = vec![];
        for dir in self.search_paths()? {
            match dir.read_dir() {
                Ok(read_dir) => {
                    for entry in read_dir.flatten() {
                        if let Ok(filename) = entry.file_name().into_string() {
                            if filename.starts_with(&lib_prefix) && filename.ends_with(&*LIB_SUFFIX)
                            {
                                let name = &filename
                                    [(lib_prefix.len())..(filename.len() - LIB_SUFFIX.len())];
                                let path = entry.path();
                                if !result.iter().any(|(_, _, n)| n == name) {
                                    // SAFETY: Call unsafe `libloading::Library::new()`.
                                    unsafe {
                                        match Library::new(path.as_os_str()) {
                                            Ok(lib) => result.push((lib, path, name.to_string())),
                                            Err(err) => warn!("{}", err),
                                        }
                                    }
                                } else {
                                    debug!(
                                        "Do not load plugin {} from {:?}: already loaded.",
                                        name, path
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => debug!(
                    "Failed to read in directory {:?} ({}). Can't use it to search for libraries.",
                    dir, err
                ),
            }
        }
        Some(result)
    }

    // _plugin_name:start
//   purpose: Extract the plugin name from a library file path by stripping prefix and suffix.
//   input:  path - Path to a library file (e.g. /usr/lib/libzenoh_plugin.so).
//   output: Option<&str> - the name between prefix and suffix ("zenoh_plugin"), or None if no filename.
//   sideEffects: none
    pub fn _plugin_name(path: &std::path::Path) -> Option<&str> {
        path.file_name().and_then(|f| {
            f.to_str().map(|s| {
                let start = if s.starts_with(LIB_PREFIX.as_str()) {
                    LIB_PREFIX.len()
                } else {
                    0
                };
                let end = s.len()
                    - if s.ends_with(LIB_SUFFIX.as_str()) {
                        LIB_SUFFIX.len()
                    } else {
                        0
                    };
                &s[start..end]
            })
        })
    }
    // _plugin_name:end
    pub fn plugin_name<P>(path: &P) -> Option<&str>
    where
        P: AsRef<std::path::Path>,
    {
        Self::_plugin_name(path.as_ref())
    }

    // str_to_canonical_path:start
//   purpose: Expand shell variables in a string path and canonicalize the result.
//   input:  s - string path (may contain ~, $VAR, etc.).
//   output: ZResult<PathBuf> - canonicalized absolute path; Err on expansion or filesystem error.
//   sideEffects: reads filesystem via canonicalize; reads env vars via shellexpand
    fn str_to_canonical_path(s: &str) -> ZResult<PathBuf> {
        let cow_str = shellexpand::full(s)?;
        Ok(PathBuf::from(cow_str.deref()).canonicalize()?)
    }
    // str_to_canonical_path:end
}

impl Default for LibLoader {
    // default:start
//   purpose: Create a LibLoader with default LibSearchDirs paths.
//   input:  none.
//   output: LibLoader.
//   sideEffects: same as LibLoader::new(LibSearchDirs::default())
    fn default() -> Self {
        LibLoader::new(LibSearchDirs::default())
    }
    // default:end
}
