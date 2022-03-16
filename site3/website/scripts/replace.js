const replace = require("replace-in-file");
const docusaurusConfig = require("../docusaurus.config")

const CWD = process.cwd();
const docsDir = `${CWD}/docs`;
const versionedDocsDir = `${CWD}/versioned_docs`;
const pagesDir = `${CWD}/src/pages`;

function doReplace(options) {
  replace(options)
    .then((changes) => {
      if (options.dry) {
        console.log("Modified files:");
        console.log(changes.join("\n"));
      }
    })
    .catch((error) => {
      console.error("Error occurred:", error);
    });
}

variablesFrom = []
variablesTo = []
function addVariable(name, value) {
  variablesFrom.push(new RegExp("{{\\s" + name + "\\s}}", "g"))
  variablesTo.push(value)

  variablesFrom.push(new RegExp("{{\\s" + name + "}}", "g"))
  variablesTo.push(value)

  variablesFrom.push(new RegExp("{{" + name + "\\s}}", "g"))
  variablesTo.push(value)

  variablesFrom.push(new RegExp("{{" + name + "}}", "g"))
  variablesTo.push(value)
  
}

for (const fieldKey in docusaurusConfig.customFields) {
  addVariable("site." + fieldKey, docusaurusConfig.customFields[fieldKey])
}

const options = {
  files: [`${docsDir}/**/*.md`, `${versionedDocsDir}/**/*.md`, `${pagesDir}/*.md`, `${pagesDir}/**/*.md` ],
  from: variablesFrom,
  to: variablesTo,
  dry: false
};

doReplace(options);
