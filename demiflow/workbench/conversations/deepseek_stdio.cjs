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
for (const syncName of ['chmodSync', 'fchmodSync']) {
  const orig = fs[syncName];
  if (typeof orig === 'function') {
    fs[syncName] = function (...args) {
      try { return orig.apply(this, args); }
      catch (err) { if (err && err.code === 'ENOSYS') return undefined; throw err; }
    };
  }
}
for (const cbName of ['chmod', 'fchmod']) {
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
if (fs.promises && typeof fs.promises.chmod === 'function') {
  const origPromiseChmod = fs.promises.chmod.bind(fs.promises);
  fs.promises.chmod = async function (...args) {
    try { return await origPromiseChmod(...args); }
    catch (err) { if (err && err.code === 'ENOSYS') return undefined; throw err; }
  };
}
