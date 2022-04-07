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

cd site3/website
yarn run docusaurus docs:version $NEW_RELEASE
rm -rf versioned_docs/version-${LATEST_RELEASED}
rm -rf versioned_sidebars/version-${LATEST_RELEASED}-sidebars.json

# remove $LATEST_RELEASED from versions.json

```
Then you have to:
- remove $LATEST_RELEASED from versions.json
- add release notes for the new release in the `src/pages/release-notes.md` file.
- - commit and push the changes

### Major/minor release

```
NEW_RELEASE=4.15.0

cd site3/website
yarn install
yarn run docusaurus docs:version $NEW_RELEASE

```
Then you have to:
- add release notes for the new release in the `src/pages/release-notes.md` file.
- commit and push the changes


