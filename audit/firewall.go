package audit

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// The host's packet filter, read from what nft list ruleset or
// iptables-save printed. The audit asks it one question — would a new
// connection from anywhere reach this port — and answers it the way the
// kernel does: the first rule that decides, chain by chain, then the
// chain's policy. What it does not follow is said where it matters: a jump
// to a chain of the user's is not walked, and a rule matching on something
// else than the port, the source and the interface (a connection state, a
// mark) is taken as not deciding.

// Firewall is a saved ruleset.
type Firewall struct {
	Path   string
	Format string // "nft" or "iptables-save"
	// Inputs are the chains every incoming packet crosses: INPUT of the
	// filter table, or each nft chain hooked on input.
	Inputs []fwChain
	// Rules are every rule read, the inputs' included, in file order.
	Rules []fwRule
}

type fwChain struct {
	Name   string
	Accept bool // the policy
	Rules  []fwRule
}

type fwRule struct {
	Line    int
	Text    string
	Ports   [][2]int // the destination ports matched; empty when none is named
	Proto   string   // "tcp", "udp", or "" when none is named
	Limited bool     // a source address, or the loopback interface, restricts it
	Other   bool     // it matches on something else: a state, a mark
	Verdict string   // accept, drop, reject, dnat, or "" for a jump or a counter
	ToPort  int      // a DNAT's destination port
}

// matches says whether the rule decides for a new TCP connection from
// anywhere to port.
func (r fwRule) matches(port int) bool {
	if r.Limited || r.Other || (r.Proto != "" && r.Proto != "tcp") {
		return false
	}
	if len(r.Ports) == 0 {
		return r.Proto == "" // a catch-all, as "-j DROP" alone
	}
	return slices.ContainsFunc(r.Ports, func(p [2]int) bool { return p[0] <= port && port <= p[1] })
}

func parseFirewall(text string) (*Firewall, error) {
	switch {
	case strings.Contains(text, "\n-A ") || strings.HasPrefix(text, "*") || strings.Contains(text, "\n*filter"):
		return parseIptablesSave(text), nil
	case strings.Contains(text, "table ") && strings.Contains(text, "{"):
		return parseNft(text), nil
	}
	return nil, errors.New("neither nft list ruleset nor iptables-save output")
}

// parseIptablesSave reads iptables-save: "*table", ":CHAIN POLICY [n:m]",
// "-A CHAIN matches -j TARGET".
func parseIptablesSave(text string) *Firewall {
	fw := &Firewall{Format: "iptables-save"}
	table := ""
	input := -1
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "*"):
			table = line[1:]
		case table == "filter" && strings.HasPrefix(line, ":INPUT "):
			fw.Inputs = append(fw.Inputs, fwChain{Name: "INPUT", Accept: strings.Fields(line)[1] == "ACCEPT"})
			input = len(fw.Inputs) - 1
		case strings.HasPrefix(line, "-A "):
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			r := iptablesRule(f[2:])
			r.Line, r.Text = n+1, line
			fw.Rules = append(fw.Rules, r)
			if table == "filter" && f[1] == "INPUT" && input >= 0 {
				fw.Inputs[input].Rules = append(fw.Inputs[input].Rules, r)
			}
		}
	}
	return fw
}

func iptablesRule(f []string) fwRule {
	var r fwRule
	negate := false
	for i := 0; i < len(f); i++ {
		arg := ""
		if i+1 < len(f) {
			arg = f[i+1]
		}
		switch f[i] {
		case "!":
			negate = true
			continue
		case "-p", "--protocol":
			r.Proto, i = arg, i+1
		case "--dport", "--destination-port", "--dports", "--destination-ports":
			r.Ports, i = portList(arg, ",", ":"), i+1
		case "-s", "--source":
			r.Limited = r.Limited || !negate
			i++
		case "-d", "--destination":
			r.Limited = r.Limited || (!negate && loopbackAddress(arg))
			i++
		case "-i", "--in-interface":
			r.Limited = r.Limited || (!negate && arg == "lo")
			i++
		case "-m", "--match":
			i++ // the module names nothing; its options do
		case "-j", "--jump":
			r.Verdict, i = strings.ToLower(arg), i+1
			if r.Verdict != "accept" && r.Verdict != "drop" && r.Verdict != "reject" && r.Verdict != "dnat" {
				r.Verdict = ""
			}
		case "--to-destination":
			r.ToPort = hostPort(arg)
			i++
		case "--ctstate", "--state", "--mark", "--uid-owner", "--icmp-type", "--tcp-flags", "--syn":
			r.Other = true
		}
		negate = false
	}
	return r
}

// parseNft reads nft list ruleset: tables, chains in braces, a chain's
// "type … hook input …; policy …;" line, and one rule a line.
func parseNft(text string) *Firewall {
	fw := &Firewall{Format: "nft"}
	var chain *fwChain
	isInput := false
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		f := strings.Fields(line)
		switch {
		case len(f) == 0:
		case f[0] == "chain":
			chain, isInput = &fwChain{Name: f[1], Accept: true}, false
		case f[0] == "}":
			if chain != nil && isInput {
				fw.Inputs = append(fw.Inputs, *chain)
			}
			chain = nil
		case f[0] == "type" && chain != nil:
			isInput = strings.Contains(line, "hook input")
			if strings.Contains(line, "policy drop") {
				chain.Accept = false
			}
		case f[0] == "policy" && chain != nil:
			chain.Accept = !strings.HasPrefix(f[1], "drop")
		case chain != nil && f[0] != "table":
			r := nftRule(f)
			r.Line, r.Text = n+1, line
			fw.Rules = append(fw.Rules, r)
			chain.Rules = append(chain.Rules, r)
		}
	}
	return fw
}

func nftRule(f []string) fwRule {
	var r fwRule
	for i := 0; i < len(f); i++ {
		next := ""
		if i+1 < len(f) {
			next = f[i+1]
		}
		switch f[i] {
		case "tcp", "udp", "th":
			if next == "dport" && i+2 < len(f) {
				if f[i] != "th" {
					r.Proto = f[i]
				}
				set, used := nftValue(f[i+2:])
				r.Ports, i = portList(set, ",", "-"), i+1+used
			}
		case "saddr":
			r.Limited = r.Limited || next != "!="
		case "iifname", "iif":
			r.Limited = r.Limited || strings.Trim(next, `"`) == "lo"
		case "ct", "mark", "skuid", "icmp", "icmpv6":
			r.Other = true
		case "l4proto":
			r.Proto = next
		case "accept", "drop", "reject":
			r.Verdict = f[i]
		case "dnat":
			r.Verdict = "dnat"
			for _, t := range f[i+1:] {
				if p := hostPort(t); p > 0 {
					r.ToPort = p
				}
			}
		}
	}
	return r
}

// nftValue reads a value that may be a set, "{ 22, 5432 }", and says how
// many fields it took.
func nftValue(f []string) (value string, used int) {
	if len(f) == 0 {
		return "", 0
	}
	if f[0] != "{" {
		return f[0], 1
	}
	var parts []string
	for i, t := range f[1:] {
		if t == "}" {
			return strings.Join(parts, ""), i + 2
		}
		parts = append(parts, t)
	}
	return strings.Join(parts, ""), len(f)
}

// portList reads "22,5432,6000:6010" (or with "-" for a range); what is
// not a number is skipped, a service name included.
func portList(s, sep, span string) [][2]int {
	var out [][2]int
	for _, p := range strings.Split(s, sep) {
		lo, hi, isRange := strings.Cut(strings.TrimSpace(p), span)
		a, err := strconv.Atoi(lo)
		if err != nil {
			continue
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				continue
			}
		}
		out = append(out, [2]int{a, b})
	}
	return out
}

// hostPort reads the port of "172.17.0.2:5432", zero when there is none.
func hostPort(s string) int {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return 0
	}
	p, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return 0
	}
	return p
}

func loopbackAddress(s string) bool {
	return strings.HasPrefix(s, "127.") || s == "::1" || s == "::1/128"
}

// exposed says how a new connection from anywhere reaches port, empty when
// none does: a DNAT that publishes it — Docker's -p, which never crosses
// INPUT — or every input chain letting it through, by a rule or by its
// policy.
func (fw *Firewall) exposed(port int) string {
	for _, r := range fw.Rules {
		if r.Verdict == "dnat" && !r.Limited && (r.matches(port) || (r.ToPort == port && len(r.Ports) > 0)) {
			return fmt.Sprintf("published by a DNAT, %s line %d: %s", fw.Path, r.Line, r.Text)
		}
	}
	if len(fw.Inputs) == 0 {
		return "no input chain filters it in " + fw.Path
	}
	var why []string
	for _, c := range fw.Inputs {
		decided := false
		for _, r := range c.Rules {
			if r.Verdict == "" || r.Verdict == "dnat" || !r.matches(port) {
				continue
			}
			if r.Verdict != "accept" {
				return ""
			}
			why = append(why, fmt.Sprintf("accepted from anywhere, %s line %d: %s", fw.Path, r.Line, r.Text))
			decided = true
			break
		}
		if decided {
			continue
		}
		if !c.Accept {
			return ""
		}
		why = append(why, "let through by the policy of chain "+c.Name+", which accepts")
	}
	return strings.Join(why, "; ")
}

// listensBeyondLoopback says whether listen_addresses admits other
// machines; true when it was not read, as the server's port is then all
// the audit knows.
func (pg *Postgres) listensBeyondLoopback() bool {
	s, ok := pg.setting("listen_addresses")
	if !ok {
		return true
	}
	for a := range strings.SplitSeq(s.Value, ",") {
		switch a = strings.TrimSpace(a); a {
		case "localhost", "127.0.0.1", "::1", "":
		default:
			return true
		}
	}
	return false
}

func init() {
	Register(Rule{
		Name:  "firewall/postgres-exposed",
		Needs: Selection{Components: {"firewall"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   "https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-LISTEN-ADDRESSES",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Firewall == nil || !t.Postgres.listensBeyondLoopback() {
				return nil
			}
			port := 5432
			if n, ok := t.Postgres.integer("port"); ok {
				port = int(n)
			}
			how := t.Firewall.exposed(port)
			if how == "" {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("PostgreSQL's port %d reachable from any address: %s", port, how),
				Proposed: "accept the port from the application's network alone (a source address on the rule), or publish it on 127.0.0.1",
				Why: "a database port open to every address is tried by every scanner on the " +
					"network; pg_hba.conf then stands alone between them and the data, and a " +
					"trust or md5 line there becomes the whole defence",
			}}
		},
	})
}
