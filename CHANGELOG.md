# Changelog

## [0.2.0](https://github.com/mlagarrigue/sluice/compare/v0.1.1...v0.2.0) (2026-10-10)


### ⚠ BREAKING CHANGES

* **quic:** rotate keys per RFC 9001 §6
* **web:** NewRouter panics on a bad table

### Features

* **postgres:** complete the codec table, Null[T] in hydrators ([f680068](https://github.com/mlagarrigue/sluice/commit/f680068714aeb5f76f22f6bc996dba628d1c0297))
* **postgres:** read a pushdown.Demand while querying ([2837674](https://github.com/mlagarrigue/sluice/commit/283767485d4286fc0559fda63e9e4a5844100fe1))
* **quic:** a socket-owning connection issues identifiers ([90c9191](https://github.com/mlagarrigue/sluice/commit/90c91913d62dbab3f0b35c2440b20cdc32478524))
* **quic:** export ListenerConfig.PreferredAddress ([19e9fcf](https://github.com/mlagarrigue/sluice/commit/19e9fcf84756373875bffec9d9a06d0031f2d9d2))
* **quic:** rotate keys per RFC 9001 §6 ([43d24f5](https://github.com/mlagarrigue/sluice/commit/43d24f595501a3b15049d6e8f5b2922fc65866c2))
* **web:** NewRouter panics on a bad table ([38509d9](https://github.com/mlagarrigue/sluice/commit/38509d9b70020650f73c74063671dacf8d7eac77))


### Bug fixes

* **ci:** give the interop runner a current Docker Engine ([6047c96](https://github.com/mlagarrigue/sluice/commit/6047c965f33221a8c3b9f6143c723309eac0cba9))
* **ci:** keep the interop runner debug output ([c6acbd7](https://github.com/mlagarrigue/sluice/commit/c6acbd71ed871ece30845620cb5710c12c208551))
* **ci:** make the interop runner scripts executable ([7c9bb81](https://github.com/mlagarrigue/sluice/commit/7c9bb81aeac8a42dee4385aab7d5df91804767b7))
* **ci:** one runner log directory per direction ([64bd986](https://github.com/mlagarrigue/sluice/commit/64bd986e006bb131b4a1894c1504829c7ae668f6))
* **httpstream:** keep reading after the peer's GOAWAY ([3960120](https://github.com/mlagarrigue/sluice/commit/3960120a474b4a8941860c86261557a1b2691d9f))
* **httpstream:** refuse a trailer section without END_STREAM ([ca36166](https://github.com/mlagarrigue/sluice/commit/ca36166938c5970103ddf928b747d1d512952226))
* **quic:** a probe sends two datagrams even for one payload ([7a94c06](https://github.com/mlagarrigue/sluice/commit/7a94c06183158e51ba8ea343f3d91dc424e49bda))
* **quic:** accept every issued identifier in long headers ([d755b24](https://github.com/mlagarrigue/sluice/commit/d755b24cd8f30c11d957eb3fbb04afc959f9ab70))
* **quic:** listen dual-stack in the interop runner endpoint ([b0175f0](https://github.com/mlagarrigue/sluice/commit/b0175f0f5cce779e7fb6513f717bf5da2ccf3461))
* **quic:** probe the client from the preferred address, both families ([42b86af](https://github.com/mlagarrigue/sluice/commit/42b86af0612442453daf68e6007f55c263533250))
* **quic:** probe timeout resends the two oldest packets ([c51203b](https://github.com/mlagarrigue/sluice/commit/c51203b058335b9e7b5032554c7aa91f4af57f9f))
* **quic:** report EOF only after the last delta is buffered ([a2e903f](https://github.com/mlagarrigue/sluice/commit/a2e903fc03048382fb6e20280684853a009d3c4c))


### Performance

* **httpstream:** coalesce streamed batches into one write ([1ed3551](https://github.com/mlagarrigue/sluice/commit/1ed355173307b41be9be2da97cf67216c65992dd))


### Documentation

* add the coverage and release badges ([4b09819](https://github.com/mlagarrigue/sluice/commit/4b09819c463a38003f3f1536703031699994e2b8))
* **httpstream:** gather the supported deployments in one place ([ffef876](https://github.com/mlagarrigue/sluice/commit/ffef876b41cefbbff468e907c11d5e87218c57bc))
* **httpstream:** HTTP/3 interop leaves the exit conditions ([0b4b15b](https://github.com/mlagarrigue/sluice/commit/0b4b15b112cef92dfb5e7d0b0bd0e13bf0d5ee33))
* **httpstream:** leave the experimental label ([7ed13d8](https://github.com/mlagarrigue/sluice/commit/7ed13d8d8d9f394ffe3ba432825b499e64fa3eff))
* **httpstream:** name the bench the label waits for ([984bd68](https://github.com/mlagarrigue/sluice/commit/984bd68550f87a4fb70f84816716d6ded57871a1))
* **httpstream:** own the per-batch cost of a streamed response ([118d2a3](https://github.com/mlagarrigue/sluice/commit/118d2a37a7dc9c42a71be6155e2e1fef8148bf35))
* **httpstream:** own the streamed-response interleaving limit ([5786dcc](https://github.com/mlagarrigue/sluice/commit/5786dcc21db847a30210738c38513b4ff4ca28fa))
* **httpstream:** publish the H2 and H3 figures ([1cc4220](https://github.com/mlagarrigue/sluice/commit/1cc4220a80a971c846b98da5359649d352066b48))
* **httpstream:** record the sentinel naming convention ([ebe18b6](https://github.com/mlagarrigue/sluice/commit/ebe18b64b018b4a65c087b99295bb568f2fca887))
* pushdown leaves the experimental list ([300571f](https://github.com/mlagarrigue/sluice/commit/300571f5a5abd7dfaa244b798945dcb47b26d7ff))
* **quic:** amplificationlimit passes on the client side ([3336ff1](https://github.com/mlagarrigue/sluice/commit/3336ff1b811a8f0aa6c9c3960ebc0d95d5012501))
* **quic:** both rebind cases pass on the client side ([ee171ae](https://github.com/mlagarrigue/sluice/commit/ee171aef13529b7dae4fdc9d7226a67f1394b307))
* **quic:** connectionmigration passes against ngtcp2's client ([0b9d8ed](https://github.com/mlagarrigue/sluice/commit/0b9d8ed0b5bb239daa5265f57bc36337b9e3b68c))
* **quic:** connectionmigration transfers, client never moves ([4fa2565](https://github.com/mlagarrigue/sluice/commit/4fa25657b208db1c9a98ac9d039eece02c497039))
* **quic:** handshake under loss passes in the interop runner ([c91ffe9](https://github.com/mlagarrigue/sluice/commit/c91ffe925548100033f2464549834e2c6bc96735))
* **quic:** key update leaves the exit conditions ([83c357f](https://github.com/mlagarrigue/sluice/commit/83c357f2b31aae0eb9a03cadfec8c1f3fb2aca23))
* **quic:** name both AEADs, the sentinel exception, udp experimental ([1e701b8](https://github.com/mlagarrigue/sluice/commit/1e701b853ccb6190573cb9e4443ff9ccd75c8ced))
* **quic:** quic-go interop leaves the exit conditions ([affc057](https://github.com/mlagarrigue/sluice/commit/affc057a8058c3ada979a84040aac57c7f886b67))
* **quic:** record the interop runner results ([663c48d](https://github.com/mlagarrigue/sluice/commit/663c48d5ebb3d751b17ecb2b6777ffb845430fea))

## [0.1.1](https://github.com/mlagarrigue/sluice/compare/v0.1.0...v0.1.1) (2026-10-09)


### Bug fixes

* **web:** bound the int conversion explicitly ([d37ecae](https://github.com/mlagarrigue/sluice/commit/d37ecae5cc338ebccaa80ec64f94994daf1b3ddc))
