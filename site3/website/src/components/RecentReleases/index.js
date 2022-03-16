import React from 'react';
import versions from "@site/versions.json"

const releases = versions.slice(1, 6)

export default function RecentReleases() {

  const mappedReleases = releases.map(r => {
    const sourceDownloadUrl = `https://archive.apache.org/dist/bookkeeper/bookkeeper-${r}/bookkeeper-${r}-src.tar.gz`
    const binaryDownloadUrl = `https://archive.apache.org/dist/bookkeeper/bookkeeper-${r}/bookkeeper-server-${r}-bin.tar.gz`
    return (
      <div>
      <h3 id={r}>Version {r}</h3>
      <table>
        <thead>
          <th>Release</th>
          <th>Link</th>
          <th>Crypto files</th>
        </thead>
        <tbody>
          <tr>
            <td>Source</td>
            <td><a href={sourceDownloadUrl}>bookkeeper-{r}-src.tar.gz</a></td>
            <td><a href={sourceDownloadUrl + '.asc'}>asc</a>, <a href={sourceDownloadUrl + '.sha1'}>sha1</a></td>
          </tr>
          <tr>
            <td>Binary</td>
            <td><a href={binaryDownloadUrl}>bookkeeper-server-{r}-bin.tar.gz</a></td>
            <td><a href={binaryDownloadUrl + '.asc'}>asc</a>, <a href={binaryDownloadUrl + '.sha1'}>sha1</a></td>
          </tr>
        </tbody>
      </table>
      </div>
    );


  });
  return [<div>{mappedReleases}</div>];
}
