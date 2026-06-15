# License

DASH is open source under the **Apache License, Version 2.0** (the "License"). You may obtain a copy of the License at:

<https://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Short summary

The Apache 2.0 license grants you the freedom to:

- **Use** DASH for any purpose — commercial, non-commercial, research, internal tooling.
- **Modify** the source code to fit your needs.
- **Distribute** the original or modified code, in source or binary form.
- **Sublicense** DASH as part of a larger work.

In return, the license requires you to:

- **Preserve the copyright notice** and the license text in any copy you distribute.
- **State significant changes** you make to the source.
- **Include a `NOTICE` file** (if one is provided) in your distribution.
- **Patent-grant clause**: each contributor grants you a patent license for their contributions. If you sue a contributor over patent claims, the grant terminates.

## What Apache 2.0 does **not** require

- **No copyleft.** You are not required to release your modifications to DASH under Apache 2.0. You can keep your changes proprietary.
- **No source distribution for binaries.** If you distribute a binary, you do not need to provide the corresponding source (though you must retain the copyright notice and the license).
- **No trademark license.** The "DASH" name and logo are not licensed for use; see the [trademark policy](https://github.com/BHAWESHBHASKAR/DASH/blob/main/TRADEMARKS.md) (forthcoming) for the rules.

## Third-party licenses

DASH depends on a number of open-source crates and libraries. The full list, with their licenses, is in `THIRD_PARTY_LICENSES.md` in the source tree. Notable dependencies:

| Crate           | License    | Notes                                                |
| --------------- | ---------- | ---------------------------------------------------- |
| `redb`          | MIT / Apache-2.0 | The embedded KV store.                          |
| `usearch`       | Apache-2.0 | The HNSW ANN index.                                  |
| `jsonwebtoken`  | MIT        | JWT verification.                                    |
| `serde`         | MIT / Apache-2.0 | Serialization framework.                        |
| `tokio`         | MIT        | Build-time only (the runtime is `std::net`).         |

A `cargo metadata --format-version=1` invocation in the source tree produces a machine-readable license manifest.

## Contributing under Apache 2.0

By submitting a contribution (a pull request, a patch, a documentation edit) to DASH, you agree to license your contribution under Apache 2.0 and to affirm the [Developer Certificate of Origin](https://developercertificate.org/) for it. See [Contributing](contributing.md) for the full workflow.
