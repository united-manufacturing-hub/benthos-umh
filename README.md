# benthos-umh

[![GitHub Actions](https://github.com/united-manufacturing-hub/benthos-umh/workflows/main/badge.svg)](https://github.com/united-manufacturing-hub/benthos-umh/actions)

This is the benthos version maintained by the United Manufacturing Hub

## Description

$
TODO$


## Usage

### Docker container

The service is distributed as a Docker container on GitHub Container Registry.
The image is built identically to the standard Benthos container
using the custom built Benthos binary.

```
$ docker run --read-only --init --publish 4195:4195 \
  --volume "$(pwd)/benthos.yaml:/benthos.yaml" \
  ghcr.io/united-manufacturing-hub/benthos-umh
```

## Development and Testing

### Quickstart

```
$ git clone https://github.com/united-manufacturing-hub/benthos-umh.git
$ cd serverless-stack
$ nvm install
$ npm install
$ sudo apt-get install zip 
$ echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' | sudo tee /etc/apt/sources.list.d/goreleaser.list
$ sudo apt update
$ sudo apt install goreleaser
$ make
$ npm test
```

Primary development tasks are defined under `scripts` in `package.json`
and available via `npm run`.
View them with

```
$ npm run
```

### Source code

The [source code] is hosted on GitHub.
Clone the project with

```
$ git clone git@github.com:united-manufacturing-hub/benthos-umh.git
```

[source code]: https://github.com/united-manufacturing-hub/benthos-umh

### Requirements

A [Go] version compatible with the one specified in `go.mod`, [GoReleaser], and [golangci-lint].

To build the [Serverless] artifacts you will need [Node.js] with [npm].

Be sure that all commands run under the correct Node version, e.g.,
if using [nvm], install the correct version with

```
$ nvm install
```

Set the active version for each shell session with

```
$ nvm use
```

Install the development dependencies with

```
$ npm install
```

[Go]: https://golang.org/
[golangci-lint]: https://golangci-lint.run/
[GoReleaser]: https://goreleaser.com/
[Node.js]: https://nodejs.org/
[npm]: https://www.npmjs.com/
[nvm]: https://github.com/creationix/nvm
[Serverless]: https://serverless.com/

### Benthos Configuration and Testing

- Benthos configuration and unit tests live side-by-side in the `config` directory.
- Configuration is bundled with the deployed artifact
  to avoid the AWS environment variable 4K size limit.
- For each item in the `benthos.artifacts` array in `package.json`,
  a Serverless zip artifact will be built to `dist` containing
  the Benthos binary and the corresponding config.

#### Adding a new function

To add a new Serverless function nameed, e.g., `foo`:

1. Create `config/foo.yaml` and `config/foo_benthos_test.yaml`.
2. Update the `benthos.artifacts` array in `package.json` to include `foo`.
3. Set the new Serverless function's `package.artifact` to `dist/foo.zip`.

### Deployment

Serverless deployment is triggered by a release repository_dispatch on GitHub Actions.

Deployment may be triggered using using a [release workflow_dispatch on GitHub Actions].

[release workflow_dispatch on GitHub Actions]: https://github.com/united-manufacturing-hub/benthos-umh/actions?query=workflow%3Arelease

## GitHub Actions

_GitHub Actions should already be configured: this section is for reference only._

The following repository secrets must be set on [GitHub Actions]:

- `AWS_DEFAULT_REGION`: The AWS region Serverless will deploy to.
- `AWS_ACCESS_KEY_ID`: AWS access key ID.
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key.
- `GH_USER`: The GitHub user's username to pull and push containers.
- `GH_TOKEN`: A personal access token that can trigger workflows.
- `GPG_PRIVATE_KEY`: The GitHub user's [GPG private key].
- `GPG_PASSPHRASE`: The GitHub user's GPG passphrase.

These must be set manually.

### Secrets for Optional GitHub Actions

The version and format GitHub actions
require a user with write access to the repository,
including access to trigger workflows.
Set these additional secrets to enable the action:

- `GH_TOKEN`: A personal access token for the user.
- `GIT_USER_NAME`: The GitHub user's real name.
- `GIT_USER_EMAIL`: The GitHub user's email.

[GitHub Actions]: https://github.com/features/actions
[GPG private key]: https://github.com/marketplace/actions/import-gpg#prerequisites

## Contributing

Please submit and comment on bug reports and feature requests.

To submit a patch:

1. Fork it (https://github.com/united-manufacturing-hub/benthos-umh/fork).
2. Create your feature branch (`git checkout -b my-new-feature`).
3. Make changes.
4. Commit your changes (`git commit -am 'Add some feature'`).
5. Push to the branch (`git push origin my-new-feature`).
6. Create a new Pull Request.

## License

This Benthos plugin is licensed under the MIT license.

## Warranty

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
