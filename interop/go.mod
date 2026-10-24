module github.com/mlagarrigue/sluice/interop

go 1.26.0

require (
	github.com/mlagarrigue/sluice v0.0.0
	github.com/quic-go/quic-go v0.64.1
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/mlagarrigue/sluice => ../
