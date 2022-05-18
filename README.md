# Model checking a toy crdt with Stateright

Based off of the first (broken) version of [Martin's tweet](https://twitter.com/martinkl/status/1327020435419041792?ref_src=twsrc%5Etfw%7Ctwcamp%5Eembeddedtimeline%7Ctwterm%5Eprofile%3Amartinkl%7Ctwgr%5EeyJ0ZndfZXhwZXJpbWVudHNfY29va2llX2V4cGlyYXRpb24iOnsiYnVja2V0IjoxMjA5NjAwLCJ2ZXJzaW9uIjpudWxsfSwidGZ3X3NlbnNpdGl2ZV9tZWRpYV9pbnRlcnN0aXRpYWxfMTM5NjMiOnsiYnVja2V0IjoiaW50ZXJzdGl0aWFsIiwidmVyc2lvbiI6bnVsbH0sInRmd190d2VldF9yZXN1bHRfbWlncmF0aW9uXzEzOTc5Ijp7ImJ1Y2tldCI6InR3ZWV0X3Jlc3VsdCIsInZlcnNpb24iOm51bGx9fQ%3D%3D&ref_url=https%3A%2F%2Fmartin.kleppmann.com%2F2020%2F07%2F06%2Fcrdt-hard-parts-hydra.html)

## Running

Add the `--broken` flag to any run to run it with the non-working version. The fixed version runs by default.

### Web viewer

```sh
cargo run --release -- serve
```

### Checker

```sh
cargo run --release -- check-bfs # or check-dfs
```

## Interesting runs

```sh
cargo run --release -- serve --broken # shows the original bug from the twitter post
```

```sh
cargo run --release -- serve # shows that the bug does get fixed with the updated version
```

```sh
cargo run --release -- serve --servers 3 # shows a new bug due to delete's context not staying around, caused by not having causal broadcast!
```
