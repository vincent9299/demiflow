// ACP owns stdout. Harness 0.2.1-rc.2's account catalog uses console.info;
// route diagnostic console methods to the already bounded stderr reader.
// Protocol frames use process.stdout directly and remain strictly decoded.
for (const method of ['log', 'info', 'debug', 'warn']) {
  console[method] = console.error.bind(console);
}

// Under Landlock ABI 1 + seccomp confinement, chmod/fchmod intentionally return
// ENOSYS in the kernel to prevent same-UID metadata tampering on read-only files.
// Harness's built-in write/edit tools chmod their newly created temp files purely
// to match mode bits; ignore ONLY ENOSYS so writes in Landlock-writable dirs succeed
// without relaxing kernel seccomp.
const fs = require('fs');
for (const syncName of ['chmodSync', 'fchmodSync', 'lchmodSync']) {
  const orig = fs[syncName];
  if (typeof orig === 'function') {
    fs[syncName] = function (...args) {
      try { return orig.apply(this, args); }
      catch (err) { if (err && err.code === 'ENOSYS') return undefined; throw err; }
    };
  }
}
for (const cbName of ['chmod', 'fchmod', 'lchmod']) {
  const orig = fs[cbName];
  if (typeof orig === 'function') {
    fs[cbName] = function (...args) {
      const cb = args[args.length - 1];
      if (typeof cb === 'function') {
        args[args.length - 1] = function (err, ...rest) {
          if (err && err.code === 'ENOSYS') return cb.call(this, null, ...rest);
          return cb.call(this, err, ...rest);
        };
      }
      return orig.apply(this, args);
    };
  }
}
if (fs.promises) {
  for (const pName of ['chmod', 'lchmod']) {
    const orig = fs.promises[pName];
    if (typeof orig === 'function') {
      fs.promises[pName] = async function (...args) {
        try { return await orig.apply(this, args); }
        catch (err) { if (err && err.code === 'ENOSYS') return undefined; throw err; }
      };
    }
  }
  if (typeof fs.promises.open === 'function') {
    const origOpen = fs.promises.open.bind(fs.promises);
    let patchedFh = false;
    fs.promises.open = async function (...args) {
      const fh = await origOpen(...args);
      if (!patchedFh && fh) {
        const proto = Object.getPrototypeOf(fh);
        if (proto && typeof proto.chmod === 'function') {
          const origFhChmod = proto.chmod;
          proto.chmod = async function (...cargs) {
            try { return await origFhChmod.apply(this, cargs); }
            catch (err) { if (err && err.code === 'ENOSYS') return undefined; throw err; }
          };
          patchedFh = true;
        }
      }
      return fh;
    };
  }
}

// When TMPDIR and the project root reside on different mountpoints, atomic
// rename(tmp, target) returns EXDEV. Fall back ONLY on EXDEV by reading the
// source bytes first, writing a temp file in the destination's OWN directory,
// and doing an atomic same-mountpoint rename(sameDirTmp, target). This never
// calls copy_file_range/fchmod (which can return ENOSYS under seccomp) and
// never truncates or deletes target if any step fails.
const _path = require('path');
const _crypto = require('crypto');
function _sameDirTemp(targetPath) {
  const dir = _path.dirname(String(targetPath));
  const base = _path.basename(String(targetPath));
  return _path.join(dir, '.' + base + '.df-tmp-' + process.pid + '-' + _crypto.randomBytes(6).toString('hex'));
}
if (typeof fs.renameSync === 'function') {
  const origRenameSync = fs.renameSync.bind(fs);
  fs.renameSync = function (oldPath, newPath) {
    try { return origRenameSync(oldPath, newPath); }
    catch (err) {
      if (err && err.code === 'EXDEV') {
        const data = fs.readFileSync(oldPath);
        const tmp = _sameDirTemp(newPath);
        try {
          fs.writeFileSync(tmp, data);
          origRenameSync(tmp, newPath);
        } catch (inner) {
          try { fs.unlinkSync(tmp); } catch (_) {}
          throw inner;
        }
        try { fs.unlinkSync(oldPath); } catch (_) {}
        return undefined;
      }
      throw err;
    }
  };
}
if (typeof fs.rename === 'function') {
  const origRename = fs.rename.bind(fs);
  fs.rename = function (oldPath, newPath, cb) {
    return origRename(oldPath, newPath, (err) => {
      if (err && err.code === 'EXDEV') {
        return fs.readFile(oldPath, (readErr, data) => {
          if (readErr) return cb(readErr);
          const tmp = _sameDirTemp(newPath);
          return fs.writeFile(tmp, data, (writeErr) => {
            if (writeErr) {
              return fs.unlink(tmp, () => cb(writeErr));
            }
            return origRename(tmp, newPath, (renErr) => {
              if (renErr) {
                return fs.unlink(tmp, () => cb(renErr));
              }
              return fs.unlink(oldPath, () => cb(null));
            });
          });
        });
      }
      return cb(err);
    });
  };
}
if (fs.promises && typeof fs.promises.rename === 'function') {
  const origPromiseRename = fs.promises.rename.bind(fs.promises);
  fs.promises.rename = async function (oldPath, newPath) {
    try { return await origPromiseRename(oldPath, newPath); }
    catch (err) {
      if (err && err.code === 'EXDEV') {
        const data = await fs.promises.readFile(oldPath);
        const tmp = _sameDirTemp(newPath);
        try {
          await fs.promises.writeFile(tmp, data);
          await origPromiseRename(tmp, newPath);
        } catch (inner) {
          try { await fs.promises.unlink(tmp); } catch (_) {}
          throw inner;
        }
        try { await fs.promises.unlink(oldPath); } catch (_) {}
        return undefined;
      }
      throw err;
    }
  };
}
