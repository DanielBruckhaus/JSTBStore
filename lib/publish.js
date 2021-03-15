const fs = require('fs');

const args = process.argv.slice(2);

const libPath = args[0];

const libJson = fs.readFileSync(libPath);
const lib = JSON.parse(libJson);

for(const {basePath, files} of lib) {
  console.log(`Processing ${basePath}...`)

  for(const file of files) {
    console.log(file);
    try {
      const contents = fs.readFileSync(`${basePath}\\${file}`);
    }
    catch(ex) {
      console.error(`Failed processing ${file}`);
      console.error(ex);
    }
  }
}
