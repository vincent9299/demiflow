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
