// ACP owns stdout. Harness 0.2.1-rc.2's account catalog uses console.info;
// route diagnostic console methods to the already bounded stderr reader.
// Protocol frames use process.stdout directly and remain strictly decoded.
for (const method of ['log', 'info', 'debug', 'warn']) {
  console[method] = console.error.bind(console);
}
