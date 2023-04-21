# Website

## How to change the documentation
The "next" release doc is under `docs` directory. If you need to edit the sidebar, you have to modify `sidebars.json` file.

## How to add a new release

### Patch release

For each minor release only the latest patch version documentation is kept.

```
export LATEST_RELEASED=4.14.4
export NEW_RELEASE=4.14.5

./site3/website/scripts/release-minor.sh
```
Then you have to:
- add release notes for the new release in the `./site3/website/src/pages/release-notes.md` file.

### Major/minor release

```
export NEW_RELEASE=4.15.0

./site3/website/scripts/release-major.sh
```
Then you have to:
- add release notes for the new release in the `./site3/website/src/pages/release-notes.md` file.
