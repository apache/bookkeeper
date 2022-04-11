# Website

## How to change the documentation
The "next" release doc is under `docs` directory. If you need to edit the sidebar, you have to modify `sidebars.json` file.

## How to add a new release
### Patch release
In order to add the release you have to follow the [official Docusaurus guide](https://docusaurus.io/docs/versioning#tagging-a-new-version)

Ensure you have [yarn](https://classic.yarnpkg.com/lang/en/docs/install) installed.

For each minor release only the latest patch version documentation is kept.

```
LATEST_RELEASED=4.14.4
NEW_RELEASE=4.14.5

./site3/website/scripts/release-minor.sh
```
Then you have to:
- add release notes for the new release in the `src/pages/release-notes.md` file.

### Major/minor release

```
NEW_RELEASE=4.15.0

./site3/website/scripts/release-major.sh
```
Then you have to:
- add release notes for the new release in the `src/pages/release-notes.md` file.
