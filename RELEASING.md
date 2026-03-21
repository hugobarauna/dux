# How to release

Because we use
[`RustlerPrecompiled`](https://hexdocs.pm/rustler_precompiled/RustlerPrecompiled.html),
releasing is more involved than a standard Hex package.

1. Pick the new release `version`.

    * We follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
    * Should be the current version in `mix.exs` with `-dev` removed.

1. Begin drafting a new release.

    1. Go to https://github.com/elixir-dux/dux/releases.
    1. Click "Draft a new release".
    1. Under "Select tag", set the tag to `v{version}`, e.g. `v0.1.0`.
    1. Keep the target branch as `main`.
    1. Click "Generate release notes".
    1. Stop here. Wait until later to actually publish the release.

1. Open a PR with any changes needed for the release. Must include:

    * Updating the `version` in `mix.exs`
    * Updating the `version` in any other files that reference it:
        * `README.md` (installation section)
        * `guides/getting-started.livemd` (Mix.install)
        * `guides/distributed-queries.livemd` (Mix.install)
        * `guides/graph-analytics.livemd` (Mix.install)
    * Updating the `CHANGELOG.md` to reflect the release

1. Merge the PR.

1. On the release draft page, click "Publish release".

1. Publishing the release will kick off the "Build precompiled NIFs" GitHub
   Action. Wait for this to complete.

    * It usually takes around 20-40 minutes.
    * Builds 6 targets: macOS (arm64, x86_64), Linux (gnu, musl), Windows.

1. Generate the artifact checksums.

    1. Ensure you have the latest version of `main` (post PR merge).
    1. Remove any intermediate builds:
        ```
        rm -rf native/dux/target
        ```
    1. Download all the artifacts and generate the checksums:
        ```
        DUX_BUILD=true mix rustler_precompiled.download Dux.Native --all --print
        ```

1. Paste the checksums into the release description on GitHub.

    1. Go to the release published earlier at the top of
       https://github.com/elixir-dux/dux/releases.
    1. Click the "Edit" pencil icon.
    1. At the bottom, paste the SHA256 contents under the heading "SHA256 of the
       artifacts" (ensure the contents are formatted to look like code).

1. Run `mix hex.publish`.

    1. Double check the dependencies and files.
    1. Enter "Y" to confirm.
    1. Discard the auto-generated `.exs` file beginning with `checksum`.

1. Bump the version in `mix.exs` and add the `-dev` flag to the end.

    * Example: `0.1.0` to `0.1.1-dev`.
    * Can either open up a PR or push directly to `main`.
