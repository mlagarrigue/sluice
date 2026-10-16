# Changelog

## [0.3.0](https://github.com/mlagarrigue/sluice/compare/v0.2.2...v0.3.0) (2026-10-16)


### ⚠ BREAKING CHANGES

* **postgres:** name the protocol version, floor and ceiling
* **quic:** count MaxConnsPerIP by address prefix
* **quic:** drop PeerClosed, read the keepalive back
* **quic:** Stream refuses an identifier past the limit
* **quic:** take the first datagram's ECN in Accept
* **httpstream:** drain ServeH2 on cancellation
* **httpstream:** merge H2Config into Config
* **httpstream:** add Server with normal and forced stop
* **httpstream:** WriteBatch announces Keep-Alive
* **postgres:** refuse a negative statement cache bound
* **distinct:** panic on a negative window
* **parallel:** panic on a negative depth or worker count
* **core:** panic on a negative size or count
* **web:** refuse a negative MaxTokenBytes
* **quic:** refuse a negative listener bound
* **httpstream:** refuse an out-of-range config value
* **postgres:** refuse an out-of-range batch bound
* **postgres:** a byte-bounded batch closes, not fails
* **postgres:** Connect replaces Startup
* **postgres:** protocol 3.2 and variable-length cancel keys
* **web:** Shutdown drains every server, then the closers
* **quic:** Err names why a connection ended
* **quic:** OpenStream takes a context and waits for credit
* **quic:** AcceptStream takes a context
* **postgres:** one round trip for a known small read

### Features

* **bench:** a hand JSON encoder on the study's server ([dd9f5b9](https://github.com/mlagarrigue/sluice/commit/dd9f5b9203db46bbb7360beb21d7dbfa7f161d5b))
* **bench:** gzip and deflate on the study's server, axis F ([8571c86](https://github.com/mlagarrigue/sluice/commit/8571c863cb77665c242a78c29f968cff4277d5e5))
* **bench:** httpstream-h3 on the study's server, axis A ([d3fad20](https://github.com/mlagarrigue/sluice/commit/d3fad203115cc4ddeaf8a0420720810d93de77ef))
* **bench:** pgx under the study's gateway, axis B ([11dd1c4](https://github.com/mlagarrigue/sluice/commit/11dd1c4323dd13f4b654ef0f19bb4dd1ff3fa322))
* **bench:** the list in a stream on the study's server, axis E ([c470e5c](https://github.com/mlagarrigue/sluice/commit/c470e5c2df9c6ee5537cb578b10478d45647b485))
* **bench:** the load generator speaks HTTP/2 and HTTP/3 ([06a8576](https://github.com/mlagarrigue/sluice/commit/06a857606aee8a7f17d1cf20bcb979c7621522c6))
* **bench:** the load generator's ramp and budget ([ab540d2](https://github.com/mlagarrigue/sluice/commit/ab540d2e8096f339edac8fdbe7b3296db93dbdee))
* **bench:** the study's load generator, h1 skeleton ([8cd896b](https://github.com/mlagarrigue/sluice/commit/8cd896b9517aa0d4eca7f2bc623285c27cca0599))
* **bench:** the study's server, one flag per axis ([c161577](https://github.com/mlagarrigue/sluice/commit/c16157768d81287f59cbcc2c04b9a5dc51d76f95))
* **bench:** the trivial server, the generator's own ceiling ([03462dc](https://github.com/mlagarrigue/sluice/commit/03462dcca32036453bdcc4fc1994233c70c86771))
* **core:** panic on a negative size or count ([372329f](https://github.com/mlagarrigue/sluice/commit/372329f7e7fd7ffdf281d1827c791d512fb135d9))
* **distinct:** panic on a negative window ([3cf7f66](https://github.com/mlagarrigue/sluice/commit/3cf7f6669996c6fbdfab6376237b32d7e0f9d423))
* **httpstream:** add Config.MaxConnsPerIP ([d7dd05c](https://github.com/mlagarrigue/sluice/commit/d7dd05c3073a73e7ebf31e21065adcc062beb1da))
* **httpstream:** add KeepAlive, close h3 with H3_NO_ERROR ([18887c6](https://github.com/mlagarrigue/sluice/commit/18887c646927e19d5645dff702f6496a1b34c5dc))
* **httpstream:** add Server with normal and forced stop ([4f1e365](https://github.com/mlagarrigue/sluice/commit/4f1e3656e1658469870d091390d0c7fba6f58b2e))
* **httpstream:** announce Keep-Alive timeout on HTTP/1.1 ([bf35881](https://github.com/mlagarrigue/sluice/commit/bf35881574f07a09af46b4a2ca5ca144993fc247))
* **httpstream:** Config.MaxConns caps accepted connections ([f5a8035](https://github.com/mlagarrigue/sluice/commit/f5a8035f8d0f4730bfebecae4dec8d54a156e107))
* **httpstream:** configurable HTTP/2 flow-control window ([83396f7](https://github.com/mlagarrigue/sluice/commit/83396f714c72de725c9a31ae97abb70a33204f24))
* **httpstream:** Date on every response ([a6ee201](https://github.com/mlagarrigue/sluice/commit/a6ee201733c5e6b72569be9ac6782c5647a1b81e))
* **httpstream:** declare a streamed ContentLength ([43e71a4](https://github.com/mlagarrigue/sluice/commit/43e71a47aa2921c3aa111f2c0f73f36951945e9b))
* **httpstream:** deliver request trailers ([228c449](https://github.com/mlagarrigue/sluice/commit/228c449829fb74e96645eec767545ec81c2f602f))
* **httpstream:** drain h1 connections on shutdown ([333c017](https://github.com/mlagarrigue/sluice/commit/333c01728205853fbfdb62e02f0e661156c264a2))
* **httpstream:** drain h2 streams on shutdown ([6921e95](https://github.com/mlagarrigue/sluice/commit/6921e95eeea9aca27786f72e0dce2f6418bbaf91))
* **httpstream:** drain h3 requests on shutdown ([4c62c8c](https://github.com/mlagarrigue/sluice/commit/4c62c8c7df8bb70f98fba99378bd401d6ecae5b1))
* **httpstream:** drain ServeH2 on cancellation ([735873e](https://github.com/mlagarrigue/sluice/commit/735873ee6764dda7060315c016fb7306e569532c))
* **httpstream:** merge H2Config into Config ([e8847f8](https://github.com/mlagarrigue/sluice/commit/e8847f85c73919fee8a5cbd2e8673b2b5dc12b9e))
* **httpstream:** refuse an out-of-range config value ([f8478ea](https://github.com/mlagarrigue/sluice/commit/f8478eae266c428051a4afb7af972c4fc25bb44a))
* **httpstream:** send response trailers ([36fa07d](https://github.com/mlagarrigue/sluice/commit/36fa07d6a033983fbf492cf9e3483f2c2ada40d8))
* **httpstream:** write h2 per stream, add WriteProgressBytes ([a9b4ba2](https://github.com/mlagarrigue/sluice/commit/a9b4ba2aff3bd14b651450079e28d9ff0d6ecb7b))
* **httpstream:** WriteBatch announces Keep-Alive ([9a24bf1](https://github.com/mlagarrigue/sluice/commit/9a24bf1ad57a0a5392812365b45ee47be810dc51))
* **parallel:** panic on a negative depth or worker count ([cd7dce7](https://github.com/mlagarrigue/sluice/commit/cd7dce7b929f9b29e608051ec135f9c12ff0c6f3))
* **postgres:** a byte-bounded batch closes, not fails ([32d8386](https://github.com/mlagarrigue/sluice/commit/32d83861035c682865c2530b2de60ba0ddb21120))
* **postgres:** cancel every wait on the server ([7674dfa](https://github.com/mlagarrigue/sluice/commit/7674dfacd4428a76310a0a99f3e57e8b8f90c46d))
* **postgres:** cancel the running statement when ctx ends ([0d992da](https://github.com/mlagarrigue/sluice/commit/0d992da887d440b48e66a2fec8e670948137ed42))
* **postgres:** complete Error fields, plain auth failures ([a0cec3e](https://github.com/mlagarrigue/sluice/commit/a0cec3e04a4bd07136fcee6cb9a4f17b16f38271))
* **postgres:** Connect replaces Startup ([0ddc63b](https://github.com/mlagarrigue/sluice/commit/0ddc63bebe0aab3c7c6933f364e8f60acdd5556a))
* **postgres:** Exec with parameters and rows affected ([ed2e50d](https://github.com/mlagarrigue/sluice/commit/ed2e50df8ad4fa247d30b9c147617104f7ad4b76))
* **postgres:** name the protocol version, floor and ceiling ([27236c3](https://github.com/mlagarrigue/sluice/commit/27236c3e5af0f8d32e15e983ac793bcd8c1ab10a))
* **postgres:** one round trip for a known small read ([dd787ed](https://github.com/mlagarrigue/sluice/commit/dd787ed4516967aedac195ee4976aeaebf3dae08))
* **postgres:** protocol 3.2 and variable-length cancel keys ([cf4e082](https://github.com/mlagarrigue/sluice/commit/cf4e082442b8285441c4b585bc92485a9c0bcd97))
* **postgres:** QueryConfig.BatchBytes bounds a batch in bytes ([9c0e222](https://github.com/mlagarrigue/sluice/commit/9c0e222f72d94cf50d4c1ca1e1474d68bdd936de))
* **postgres:** refuse a negative statement cache bound ([655ba3b](https://github.com/mlagarrigue/sluice/commit/655ba3b510c227b30bafb440b8837c0e50f8122f))
* **postgres:** refuse an out-of-range batch bound ([a3b3f23](https://github.com/mlagarrigue/sluice/commit/a3b3f23a2978caa0a0376be65c70b1c8b503a877))
* **postgres:** report the tag of a streamed query ([a6ad652](https://github.com/mlagarrigue/sluice/commit/a6ad65280acce079e0af82a8fb80d97dbfd27600))
* **postgres:** RequireAuth and RequireChannelBinding ([bbf4745](https://github.com/mlagarrigue/sluice/commit/bbf4745b6d217d8cac97006ac8e34928530a48a1))
* **postgres:** StartupConfig.MaxMessage lifts the 64 MiB message limit ([98f1e65](https://github.com/mlagarrigue/sluice/commit/98f1e6549606c9fe3c2a68095017c9d60701d7d9))
* **quic:** AcceptStream takes a context ([2738054](https://github.com/mlagarrigue/sluice/commit/27380546671a36d11b375c23cfaae9cb4ff56de2))
* **quic:** add Conn.Ping, a measured round trip ([55a2b2d](https://github.com/mlagarrigue/sluice/commit/55a2b2d51bd9b3463e54743a452ccdebaace1be4))
* **quic:** add SetClosingPeriod and draining state ([77e5ac7](https://github.com/mlagarrigue/sluice/commit/77e5ac75907eeafc8a2e0c2d7062be53e1c0d515))
* **quic:** BBR controller, draft-ietf-ccwg-bbr ([07d4a4e](https://github.com/mlagarrigue/sluice/commit/07d4a4efe4f0ed45dd31cf1ce04756445a7a5c4c))
* **quic:** BBRv1 congestion controller ([97442f4](https://github.com/mlagarrigue/sluice/commit/97442f4a812973fc2f8bb762c2caa7571ebee219))
* **quic:** choose slow start and fast convergence ([c20cd73](https://github.com/mlagarrigue/sluice/commit/c20cd73fa0c855950a97bf1526ba541adf242dcc))
* **quic:** ConnectionState, LocalAddr, RemoteAddr and Listener.Addr ([5f17c63](https://github.com/mlagarrigue/sluice/commit/5f17c638e85b268c88ae3a8977aa487861c98bfe))
* **quic:** controllers respond to ECN-CE marks ([a729fad](https://github.com/mlagarrigue/sluice/commit/a729fadc0c1208f4a40d8547fc12cfa6fe163d54))
* **quic:** count MaxConnsPerIP by address prefix ([a4178a7](https://github.com/mlagarrigue/sluice/commit/a4178a7cded47367d62bebdfb179ce373632d175))
* **quic:** count persistent congestion and retransmitted bytes ([b2c56c6](https://github.com/mlagarrigue/sluice/commit/b2c56c6ad620e22b9f828e5fcd00a03a890bf961))
* **quic:** CUBIC congestion control, chosen per connection ([45b6a62](https://github.com/mlagarrigue/sluice/commit/45b6a6270be8e4a42cfaac1043b2e210a2251e4d))
* **quic:** delivery-rate samples per acknowledgement ([ecc4531](https://github.com/mlagarrigue/sluice/commit/ecc453166fd5a0ad0a76cc8a9ab5329cfd47f814))
* **quic:** disable_active_migration is read and announced ([3a6cf59](https://github.com/mlagarrigue/sluice/commit/3a6cf59a0dad452819454b6e44c9d4380866c376))
* **quic:** DPLPMTUD probes lift datagrams past 1200 bytes ([1a6444d](https://github.com/mlagarrigue/sluice/commit/1a6444d3c285047988c11905052302dd664c2add))
* **quic:** drop PeerClosed, read the keepalive back ([7613a95](https://github.com/mlagarrigue/sluice/commit/7613a95441e0421b738a35dd74deec620a0fb30c))
* **quic:** Err names why a connection ended ([b913611](https://github.com/mlagarrigue/sluice/commit/b913611eb5b26df95707c6a7122200d7301fab04))
* **quic:** HyStart++ slow-start exit for CUBIC ([2ac41cc](https://github.com/mlagarrigue/sluice/commit/2ac41ccd195a88b59871dad4042336550606ca7e))
* **quic:** ListenerConfig caps connections, total and per IP ([7c70a0f](https://github.com/mlagarrigue/sluice/commit/7c70a0f0f27c8378cacac14c5d5b80528a768897))
* **quic:** ListenerConfig.HandshakeTimeout bounds a handshake ([b4126f6](https://github.com/mlagarrigue/sluice/commit/b4126f65f5a38551d918a95640f9e3cd3e95a9cc))
* **quic:** OpenStream takes a context and waits for credit ([01548d2](https://github.com/mlagarrigue/sluice/commit/01548d218a6cfde0632fdb40842467a7a129b4a4))
* **quic:** OpenUniStream opens a stream under the peer's grant ([c1b27a5](https://github.com/mlagarrigue/sluice/commit/c1b27a504948557d7b6a91646f6194ddaefb9150))
* **quic:** read and mark ECN on Darwin and Windows ([1085528](https://github.com/mlagarrigue/sluice/commit/108552877ff8e0acc17a739dce5f155773daca35))
* **quic:** read ECN and count it in ACK frames ([40fd0a3](https://github.com/mlagarrigue/sluice/commit/40fd0a36db523340ff3b310dba5fbbaf9fac0610))
* **quic:** read packet info on Darwin ([405bfc6](https://github.com/mlagarrigue/sluice/commit/405bfc6cb03e1f1052a38fd656a3fdce99cfdc4b))
* **quic:** read packet info on Windows ([9939f0f](https://github.com/mlagarrigue/sluice/commit/9939f0fb07fb5e7a176f1c41491403e4cf8baa4d))
* **quic:** refuse a negative listener bound ([72c4625](https://github.com/mlagarrigue/sluice/commit/72c462535674beca0cee661bb867901f0a4d186b))
* **quic:** SetKeepAlive pings under the idle timeout ([f50d521](https://github.com/mlagarrigue/sluice/commit/f50d5215eb4afeb61dddae089ef20f4a07163968))
* **quic:** Shutdown waits for in-flight data to be acked ([cb9a189](https://github.com/mlagarrigue/sluice/commit/cb9a1899d515f37790c42abbc79932e2106d50ad))
* **quic:** ShutdownWithError, drain then application close ([a52b85b](https://github.com/mlagarrigue/sluice/commit/a52b85b43fdc661fbf57af53df3153aca2a27402))
* **quic:** Stats reports RTT, window, flight and losses ([3779aee](https://github.com/mlagarrigue/sluice/commit/3779aee2f9292bfe8c7c4ef91aa7dbf5f94a6d43))
* **quic:** Stream read and write deadlines ([7ccc3d0](https://github.com/mlagarrigue/sluice/commit/7ccc3d0ff3bda0ee17911edb1b4f90e567495175))
* **quic:** Stream refuses an identifier past the limit ([0f440d6](https://github.com/mlagarrigue/sluice/commit/0f440d6f9b6ce1eb335c384ecfddf37498708111))
* **quic:** take the first datagram's ECN in Accept ([16d8523](https://github.com/mlagarrigue/sluice/commit/16d852369b369fdf3cb6cab5f68fbc5feb352111))
* **quic:** WriteClose sends data and FIN together ([ca6d363](https://github.com/mlagarrigue/sluice/commit/ca6d36391d7ddfd182d153016f51ba9770bc0840))
* **web:** force a server Shutdown could not drain ([84c2969](https://github.com/mlagarrigue/sluice/commit/84c2969d3236442780dd855c0431c78123d971f6))
* **web:** refuse a negative MaxTokenBytes ([dae2743](https://github.com/mlagarrigue/sluice/commit/dae2743e7dd9912d9523d402fb38b751f98c71c6))
* **web:** Shutdown drains every server, then the closers ([a42f4d6](https://github.com/mlagarrigue/sluice/commit/a42f4d6ecc1f817094024f162a8cfe80e6247ca2))


### Bug fixes

* **bench:** alternate binary paths and order in A/B ([07fdc7a](https://github.com/mlagarrigue/sluice/commit/07fdc7a45de97af3bd759437d511516837e23b3c))
* **bench:** let versus.sh pair sub-benchmarks ([7175e95](https://github.com/mlagarrigue/sluice/commit/7175e95fe9a9c3f0d8d4eb2d438a84c4b8dba267))
* **ci:** track every shell script executable ([dfd40fb](https://github.com/mlagarrigue/sluice/commit/dfd40fb01984ec8079cb406070fc7a03cc441781))
* **httpstream:** 414, 431 and 505 where the RFCs put them ([5603231](https://github.com/mlagarrigue/sluice/commit/560323152ef32827c38022ea29b628a9a675991d))
* **httpstream:** 62-bit QPACK integers ([9ec0f98](https://github.com/mlagarrigue/sluice/commit/9ec0f98f8a0ed37c8d86e8035c64c0fa796bd006))
* **httpstream:** answer an oversized h3 DATA frame 413 ([3525060](https://github.com/mlagarrigue/sluice/commit/35250604e5d0ccf91c3a45474dedee4e84ccb8c2))
* **httpstream:** arm the drain before closing the listener ([5e5cf76](https://github.com/mlagarrigue/sluice/commit/5e5cf76681c457c247820b56716063ba6ff0f122))
* **httpstream:** check the whole RFC 3986 authority ([6d13c69](https://github.com/mlagarrigue/sluice/commit/6d13c6972446a77eed4ae049082230c6cb8295e8))
* **httpstream:** close the connection when SETTINGS cannot be sent ([8a701b0](https://github.com/mlagarrigue/sluice/commit/8a701b05f65f250a5d2ca9658b1e23d4c5b778f9))
* **httpstream:** connection errors where RFC 9114 requires them ([f32c557](https://github.com/mlagarrigue/sluice/commit/f32c5576450c90bf0448304dfd3b24424d578c5c))
* **httpstream:** derive HTTP/2 bounds and h3 idle time from Config ([68745ef](https://github.com/mlagarrigue/sluice/commit/68745efe19880f1977eef84a4634ca6086c7fb9e))
* **httpstream:** h1 serves what parsed before an error ([5eb79a0](https://github.com/mlagarrigue/sluice/commit/5eb79a0b934594675672268a0b19d25553ea5242))
* **httpstream:** h2 and h3 cookie crumbs are joined again ([f80ff87](https://github.com/mlagarrigue/sluice/commit/f80ff874776d9dc477b5ee7a2404858e63dc76ea))
* **httpstream:** h2 and h3 require an authority ([fcf0c15](https://github.com/mlagarrigue/sluice/commit/fcf0c1577cfc033237e7a0a95389d8cb93c68a4a))
* **httpstream:** h2 answers 431 and 413 on the stream ([605ff28](https://github.com/mlagarrigue/sluice/commit/605ff28b9c4f7fad3c7f554d3ed07bf71543ee45))
* **httpstream:** h2 write deadline needs progress ([b5f2913](https://github.com/mlagarrigue/sluice/commit/b5f2913e066fdab34c0885429d0063c711b5c0d4))
* **httpstream:** h3 answers 431 and 413 on the stream ([f2e76d6](https://github.com/mlagarrigue/sluice/commit/f2e76d6b5517de8c0414323ca9590ab885458786))
* **httpstream:** h3 responses give up after WriteTimeout ([d54f002](https://github.com/mlagarrigue/sluice/commit/d54f00293ea8cea2a46162499067f83c62103852))
* **httpstream:** half-close before closing on a refused request ([10e4e3b](https://github.com/mlagarrigue/sluice/commit/10e4e3bf3aeeb3b8d8a17f78f34bf47b43a61a5c))
* **httpstream:** Host is an authority on every protocol ([441ef37](https://github.com/mlagarrigue/sluice/commit/441ef37abb4342f316d9de8663c2bb4d3ee7308e))
* **httpstream:** lowercase response field names on h2 and h3 ([11ea5b5](https://github.com/mlagarrigue/sluice/commit/11ea5b5687f365ef58dde90339097f7cc3f78789))
* **httpstream:** refuse HTTP/2 frame types over HTTP/3 ([881d1d8](https://github.com/mlagarrigue/sluice/commit/881d1d8cfa0d55b45726c1fdcd25ef66e11a9d48))
* **httpstream:** send Date first on every response ([10bc114](https://github.com/mlagarrigue/sluice/commit/10bc114b2982337fcd8c284f0a3b4d42cf19ea86))
* **httpstream:** send the h3 GOAWAY pair on shutdown ([c99dd2f](https://github.com/mlagarrigue/sluice/commit/c99dd2fa04b2fc7d86e8a71d580c9b596fdd31f2))
* **httpstream:** Serve backs off on a temporary Accept error ([1ecaac4](https://github.com/mlagarrigue/sluice/commit/1ecaac4c28cd62b503adbaeab8e1160327a1a5c1))
* **httpstream:** serve HTTP/1.x as HTTP/1.1 ([d459277](https://github.com/mlagarrigue/sluice/commit/d459277455c8132cdd944fe754f9bca4547efebe))
* **httpstream:** ServeH3 refuses a connection without h3 ([56bf631](https://github.com/mlagarrigue/sluice/commit/56bf6318eccc56efae3582142151ab49c8a533ea))
* **httpstream:** ServeH3 returns at the grace ([a74879a](https://github.com/mlagarrigue/sluice/commit/a74879ab9e5f2348ceacc8f1ac79e5e231cfec73))
* **postgres:** break the connection on an unasked encoding ([83a49d1](https://github.com/mlagarrigue/sluice/commit/83a49d1b33ff53d2184b038e7b2a0e0503474c78))
* **postgres:** lift the handshake deadline after Startup ([04e3771](https://github.com/mlagarrigue/sluice/commit/04e377106c7253f544eb2b8d8f3c1f0a712c249c))
* **postgres:** Pipeline reads while it still writes ([9429507](https://github.com/mlagarrigue/sluice/commit/942950761b6b616d5ebeeb45b0f9bca9ea18fb8e))
* **postgres:** refuse a replication startup ([5ae5174](https://github.com/mlagarrigue/sluice/commit/5ae517428fc2f2a0b82f99bbf1890dca807cbf60))
* **postgres:** reject out-of-order SCRAM messages ([79b22a8](https://github.com/mlagarrigue/sluice/commit/79b22a8d1908317a0f99eb4001f4966bf375e238))
* **quic:** a due ACK is counted before the stream chunk ([bb2b94b](https://github.com/mlagarrigue/sluice/commit/bb2b94ba8ade9a734f4bbd6da364f4a49abebf0c))
* **quic:** a late frame on a closed stream never revives it ([d842efb](https://github.com/mlagarrigue/sluice/commit/d842efb2d307aeb758656dd73b27537b63808b40))
* **quic:** a lost CONNECTION_CLOSE is repeated in closing ([2933a72](https://github.com/mlagarrigue/sluice/commit/2933a72d560ac043951fc761738e3157bd265e7e))
* **quic:** a migrated path starts with no flight to answer for ([1ec6acd](https://github.com/mlagarrigue/sluice/commit/1ec6acdb325c44521312037b4d5aab9765336874))
* **quic:** a Retry token is consumed with its connection ([5135f71](https://github.com/mlagarrigue/sluice/commit/5135f717ba41dce7c2050ea84fc4bc15655d1efe))
* **quic:** a skipped packet number catches optimistic ACKs ([3ad9b0c](https://github.com/mlagarrigue/sluice/commit/3ad9b0ca89d8402ac3271a03cb7a3fa3bb35d9e6))
* **quic:** a token from another issuer means unvalidated ([5a983c1](https://github.com/mlagarrigue/sluice/commit/5a983c16756cd4870538f452b70de210438a384c))
* **quic:** ACKs ride data, MAX_STREAMS by eighths ([481778e](https://github.com/mlagarrigue/sluice/commit/481778e5435707f6be57ccac6447c8e9288da894))
* **quic:** bound reassembly chunks by the bytes held ([1dbd6a1](https://github.com/mlagarrigue/sluice/commit/1dbd6a12206084e592347555880d3de5c540ee1a))
* **quic:** bounds and presence checks RFC 9000 names ([972ae01](https://github.com/mlagarrigue/sluice/commit/972ae01554ca458d927a88cbaf3015b6622fcb5b))
* **quic:** count sent packets, not numbers, for loss ([36d6b81](https://github.com/mlagarrigue/sluice/commit/36d6b8137b6bca19f3d51a5f5750c609a9ae6d56))
* **quic:** cwnd only grows while it is the limit ([4c56d6b](https://github.com/mlagarrigue/sluice/commit/4c56d6b1e3d7f8d3842a52fd2098a42d1548eea8))
* **quic:** declare a time loss at the instant it matures ([c7f9363](https://github.com/mlagarrigue/sluice/commit/c7f93637761a0a940085267859544100408927e2))
* **quic:** deliver STOP_SENDING to OnStreamFrames ([52f13ea](https://github.com/mlagarrigue/sluice/commit/52f13ea9cbb8819759453ba0a3287ccb2e588eb4))
* **quic:** discarding a space resets the probe count ([0a0fa50](https://github.com/mlagarrigue/sluice/commit/0a0fa50a2d01d8aadcfd896c45c4e96f9f6ce4b8))
* **quic:** drain BBR at 0.5 and leave after three rounds ([3889806](https://github.com/mlagarrigue/sluice/commit/38898067c829e050ad2829135168a6e698603fdf))
* **quic:** draw the Retry packet's unused bits ([afadcbe](https://github.com/mlagarrigue/sluice/commit/afadcbe71e1fcb921ec8ff9c66af59ad36577a1e))
* **quic:** feed BBR's max filter per ACK ([5e18639](https://github.com/mlagarrigue/sluice/commit/5e186398d9129ce6d544f09f13475df338abe180))
* **quic:** free the socket once the read loop exits ([77ac428](https://github.com/mlagarrigue/sluice/commit/77ac4288b1a3a330167fab39789104d884c28ae8))
* **quic:** grant a peer stream back at Data Read ([250bc13](https://github.com/mlagarrigue/sluice/commit/250bc1381629b797dfea8352adef7438abcf54ba))
* **quic:** handle losses before growth on an ACK (A.7) ([5788a71](https://github.com/mlagarrigue/sluice/commit/5788a715575916c989aafed4bf7131b7529c8127))
* **quic:** idle and ack-delay parameters saturate on a millisecond ([c77c22e](https://github.com/mlagarrigue/sluice/commit/c77c22ef8500467f1f2a8b9b5c73da61867cd354))
* **quic:** keep BBR's bound on app-limited losses ([9dd35bf](https://github.com/mlagarrigue/sluice/commit/9dd35bf8032626ae390b6cb36baf32788b9916b4))
* **quic:** Listener.Close ends the accepted connections ([3485f97](https://github.com/mlagarrigue/sluice/commit/3485f97895b5b1a2f1b2cd0c4101beb05baf9557))
* **quic:** mark app-limited only on a missed send chance ([20676a3](https://github.com/mlagarrigue/sluice/commit/20676a3854e3c9cc1dc24c7a4cee32e63d1cd6e5))
* **quic:** mark BBR ProbeRTT app-limited ([a3efd0e](https://github.com/mlagarrigue/sluice/commit/a3efd0e609ceb50c2b66224ffc8c26fa34c1e029))
* **quic:** no probe timeout under the amplification limit ([ce2ed79](https://github.com/mlagarrigue/sluice/commit/ce2ed79e14d42ae083cdf1245f1a75d97d7c8f10))
* **quic:** one ECN message per datagram, by address family ([bc701a3](https://github.com/mlagarrigue/sluice/commit/bc701a3fa41ad16e504a1bf15bc9d601c6f25c27))
* **quic:** one PATH_RESPONSE datagram per packet, budgeted off-path ([6e39bda](https://github.com/mlagarrigue/sluice/commit/6e39bda85dddd2f14f8d9a3aca13a950ecb5c810))
* **quic:** owe the ACK before delivering frames ([b91bd3b](https://github.com/mlagarrigue/sluice/commit/b91bd3b450581b7cf518c4c3766ce2fe293424bf))
* **quic:** persistent congestion is a span of losses, not a count ([6060e53](https://github.com/mlagarrigue/sluice/commit/6060e5371d38c6d4781bc791798912612e438217))
* **quic:** probe BBR bandwidth with precaution ([2636dc2](https://github.com/mlagarrigue/sluice/commit/2636dc291a810fcf51360dd02aa83e92095c96f6))
* **quic:** protocol violations carry their normative codes ([b574f2b](https://github.com/mlagarrigue/sluice/commit/b574f2bb4d2bcf32b59b4734388970696b791a89))
* **quic:** reserved header bits are checked after the AEAD opens ([a1b61fc](https://github.com/mlagarrigue/sluice/commit/a1b61fcc5d094e85e78b60cabef501c61d773866))
* **quic:** retired connection IDs are replaced within bounds ([a59cfd5](https://github.com/mlagarrigue/sluice/commit/a59cfd548c80a81a5788d33cbebfd64004575c33))
* **quic:** send STREAMS_BLOCKED on a try-open too ([f7a83c9](https://github.com/mlagarrigue/sluice/commit/f7a83c9127673ced64287bd8129572ad3ca72e5b))
* **quic:** stamp BBR losses per packet ([963df98](https://github.com/mlagarrigue/sluice/commit/963df987fd83a06175e47eaeb800f1f75abc1a2f))
* **quic:** the early secret is not installed as 1-RTT keys ([8ed5bc2](https://github.com/mlagarrigue/sluice/commit/8ed5bc2b365bd0c5c3dc53c4a72fca741ab05594))
* **quic:** the loss timer spares packets past the largest acked ([6128b4f](https://github.com/mlagarrigue/sluice/commit/6128b4f481add60e994475539bfd9fcc66000f16))
* **web:** bound idle keep-alive connections ([e3b86e6](https://github.com/mlagarrigue/sluice/commit/e3b86e695675cf5bd185145c1c1baa4a6205c9ea))


### Performance

* **httpstream:** one write for a buffered h3 response ([ab5665f](https://github.com/mlagarrigue/sluice/commit/ab5665f3965db540c2af73b1795ef97e620e0078))
* **postgres:** ask for two batches before delivering one ([df75704](https://github.com/mlagarrigue/sluice/commit/df75704176e781cca3e1d9c0cec708f77ea68c6a))
* **quic:** a refused Initial derives the client keys only ([292566a](https://github.com/mlagarrigue/sluice/commit/292566a867e1d007e92816d32d86ed27d92392b3))
* **quic:** queue a burst for a busy connection ([b04272a](https://github.com/mlagarrigue/sluice/commit/b04272a78a9b66f9d2ee56014cfb885bad3fe099))
* **quic:** send a write's datagrams in one call ([f5abf43](https://github.com/mlagarrigue/sluice/commit/f5abf43fe803440d31c9723436b5e5b32e09fca3))


### Documentation

* **bench:** BBR against Reno on the path profiles ([d3b77da](https://github.com/mlagarrigue/sluice/commit/d3b77da411052088db5a044d694fc077a8e10c23))
* **bench:** BBR with ECN on DeepBuffer ([ed6879a](https://github.com/mlagarrigue/sluice/commit/ed6879a5fae77a84485743d676194542cc6a3cec))
* **bench:** BBR's ceiling and 16 MiB median ([7cae100](https://github.com/mlagarrigue/sluice/commit/7cae100f918a2a9983cf8c8ee1df8fcfe690e250))
* **bench:** BBRv1 against Reno on the path profiles ([4369ea3](https://github.com/mlagarrigue/sluice/commit/4369ea39794dc2e411356631e44e05ae0b808bcf))
* **bench:** CUBIC against Reno on the path profiles ([05af9d5](https://github.com/mlagarrigue/sluice/commit/05af9d5a130fbe286dd579894b5f38fd7e4b0148))
* **bench:** CUBIC slow-start exit with HyStart++ ([17a1db9](https://github.com/mlagarrigue/sluice/commit/17a1db907efe178f085af2c4766eb18597454487))
* **bench:** report QUIC loss counts and current rtt_x ([86cdb90](https://github.com/mlagarrigue/sluice/commit/86cdb902fe1529f99cd71467bccded8ee369914d))
* **bench:** say what a Reorder loss gate would miss ([997e70a](https://github.com/mlagarrigue/sluice/commit/997e70aee8bb6a5aea15cd512ea905d76381cc96))
* **bench:** starting ceilings for the new path profiles ([bc162b4](https://github.com/mlagarrigue/sluice/commit/bc162b410aaae9c1b8cbaf6a539199c1c95e8dd6))
* **devcontainer:** explain the local overlay ([6e5fe1a](https://github.com/mlagarrigue/sluice/commit/6e5fe1a15a3b639f306ced8cec3ff466ab6ac3b1))
* **httpstream:** a streamed response has no length ([9e50b40](https://github.com/mlagarrigue/sluice/commit/9e50b408ac406abbe1025c60f37d2d0b7a8bc6e4))
* **httpstream:** ADR 0017, HTTP/2 drains ([514113b](https://github.com/mlagarrigue/sluice/commit/514113beac4063f36050af52cd14626f2ef68968))
* **httpstream:** ADR 0017, HTTP/3 drains ([cfdf182](https://github.com/mlagarrigue/sluice/commit/cfdf1829278c3443887377b4a70713045be9a90c))
* **httpstream:** ADR 0017, HTTP/3 returns at the grace ([a998160](https://github.com/mlagarrigue/sluice/commit/a99816087653192a6ca172668456098570521ad5))
* **httpstream:** ADR 0017, shutdown drains ([cbbe475](https://github.com/mlagarrigue/sluice/commit/cbbe4755bfc887219918056c73bb0c182fcf4c0f))
* **httpstream:** ADR 0018, zero values and mandatory bounds ([4b38a2f](https://github.com/mlagarrigue/sluice/commit/4b38a2f9a6ea53e5c5a5b47f404fcecdc35cf793))
* **httpstream:** MaxRequestsPerConn is HTTP/1.1 only ([a5bdefe](https://github.com/mlagarrigue/sluice/commit/a5bdefe9fbf4b1d8f2f261f47432e271cb526252))
* **httpstream:** name the protocols each Config field binds ([e4257c0](https://github.com/mlagarrigue/sluice/commit/e4257c00b1b3a52f76a6130e0b4c0e86742b05d5))
* **httpstream:** only idempotent requests replay ([4620ceb](https://github.com/mlagarrigue/sluice/commit/4620ceb308a873efc4cd27855d21cc11f322a635))
* **httpstream:** price the connection budget ([53f30fc](https://github.com/mlagarrigue/sluice/commit/53f30fc33f92eb8a51775d06d73e374d626967c5))
* **httpstream:** proxies ignore the Keep-Alive timeout ([3ebd2b2](https://github.com/mlagarrigue/sluice/commit/3ebd2b29385e409a3b782e7170a6b8b7b3888006))
* **httpstream:** refuse a negative bound in ADR 0018 ([35296c3](https://github.com/mlagarrigue/sluice/commit/35296c31fd4a5357ced80a208ce353d446ba77cd))
* **httpstream:** request bodies keep their Content-Encoding ([484869d](https://github.com/mlagarrigue/sluice/commit/484869dee2d093d2ef5d49b808f2e9b7e914bd1b))
* **httpstream:** request bodies stay bounded, not streamed ([ea00ab2](https://github.com/mlagarrigue/sluice/commit/ea00ab2db09e80fd110ec15cf86a9ecf0bd9cb64))
* **httpstream:** request trailers are validated then dropped ([231bd40](https://github.com/mlagarrigue/sluice/commit/231bd40ad28b7ff69b4867d43cbf4860cefecb7b))
* **httpstream:** the TLS config to put in front ([375bcae](https://github.com/mlagarrigue/sluice/commit/375bcae8c27b4b0cd2d514357f5fe8441b1c02a7))
* **httpstream:** why deliver waits for the wire ([1e850ef](https://github.com/mlagarrigue/sluice/commit/1e850ef89a12ab70340d4acd5b57e5a436762957))
* **postgres:** COPY reads server errors at commit ([6cb8ee2](https://github.com/mlagarrigue/sluice/commit/6cb8ee23bacafa2cd5081b7a8d2022ac77a942c3))
* **postgres:** date the two-batch measurement ([9455d95](https://github.com/mlagarrigue/sluice/commit/9455d95d620e4f22ddd28a8f829eb5f62ac8502b))
* **postgres:** explain cancelling a running query ([615e4cc](https://github.com/mlagarrigue/sluice/commit/615e4ccb58b1a66694009190bd092a8975bc1ad7))
* **postgres:** name what the wide read still pays ([02d1af5](https://github.com/mlagarrigue/sluice/commit/02d1af537a8942077815cec884e785685e14db57))
* **postgres:** time the first and last batch of a wide read ([1abcaf6](https://github.com/mlagarrigue/sluice/commit/1abcaf65be6708d01d6e5d66dfc58d89c88d7aec))
* **postgres:** TLS negotiation and limits ([3a8e1f2](https://github.com/mlagarrigue/sluice/commit/3a8e1f24d1ffae67ba3ec15db3a578c80dd44b5d))
* **quic:** cite BBR draft -06 and RFC 9002 B.7 ([40b3bab](https://github.com/mlagarrigue/sluice/commit/40b3babac7cf954126440111942015a474b73d6c))
* **quic:** correct godocs and the gated row count ([eaf500c](https://github.com/mlagarrigue/sluice/commit/eaf500c19b27486ae1282783a06e069ca77cadd0))
* **quic:** datagrams stay at 1200 bytes without DPLPMTUD ([a1b0c3d](https://github.com/mlagarrigue/sluice/commit/a1b0c3dde9bcf104d77bb7acc0f99dc8f23697c2))
* **quic:** state why BBR skips the loss undo ([e73b292](https://github.com/mlagarrigue/sluice/commit/e73b292fbdf91ca4efd762dddabc6b05b14137ee))
* **web:** ADR 0017, web.Shutdown ([62fd011](https://github.com/mlagarrigue/sluice/commit/62fd01192bdb1a1d6ad20d79cc7bb1df408f1a4e))

## [0.2.2](https://github.com/mlagarrigue/sluice/compare/v0.2.1...v0.2.2) (2026-10-13)


### Documentation

* **quic:** hold the interop exit condition in our CI ([85a6aca](https://github.com/mlagarrigue/sluice/commit/85a6aca797681b3b4eb6610d2e7fdd5c5eb433fe))
* **quic:** name why HTTP/3 bulk trails HTTP/2 ([8c02a86](https://github.com/mlagarrigue/sluice/commit/8c02a865ae4f11828cf61c6ffcf148b366df1386))

## [0.2.1](https://github.com/mlagarrigue/sluice/compare/v0.2.0...v0.2.1) (2026-10-11)


### Documentation

* **quic:** link the public interop matrix ([1b22363](https://github.com/mlagarrigue/sluice/commit/1b223630a1210a3708cc069ee3257191c0a2126d))

## [0.2.0](https://github.com/mlagarrigue/sluice/compare/v0.1.1...v0.2.0) (2026-10-11)


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
* **quic:** interop client verifies the runner's chain ([4a6ccb0](https://github.com/mlagarrigue/sluice/commit/4a6ccb03402e3a56c761e22d7818050c51bb21e7))
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
