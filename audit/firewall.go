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
// kernel does: the first rule that decides, chain by chain, jumps into the
// chains of the user's followed (ufw and firewalld keep every acceptance
// there, under a drop policy), then the chain's policy. A rule matching on
// something this reader does not interpret (a connection state, a mark, an
// address type) is taken as not deciding; a jump it cannot follow — a
// target it does not find, a loop, a condition it does not read in front
// of a chain that would accept — makes the answer "undetermined", never a
// silence that would read as a closed port.

// Firewall is a saved ruleset.
type Firewall struct {
	Path   string
	Format string // "nft" or "iptables-save"
	// Chains are every chain read, keyed by table and name.
	Chains map[string]*fwChain
	// Inputs are the keys of the chains every incoming packet crosses:
	// INPUT of the filter table, or each nft chain hooked on input.
	Inputs []string
	// Rules are every rule read, in file order.
	Rules []fwRule
}

type fwChain struct {
	Name   string
	Accept bool // the policy; a chain of the user's has none and returns
	Rules  []fwRule
}

type fwRule struct {
	Line    int
	Text    string
	Ports   [][2]int // the destination ports matched; empty when none is named
	Proto   string   // "tcp", "udp", or "" when none is named
	Limited bool     // a source address, or the loopback interface, restricts it
	Other   bool     // it matches on something this reader does not interpret
	Verdict string   // accept, drop, reject, dnat, return, jump, goto, or "" for none
	Target  string   // the chain a jump or a goto names, table-qualified
	ToPort  int      // a DNAT's destination port
}

// applies says whether the rule's conditions hold for a new TCP connection
// from anywhere to port, the unread ones aside.
func (r *fwRule) applies(port int) bool {
	if r.Limited || (r.Proto != "" && r.Proto != "tcp") {
		return false
	}
	if len(r.Ports) == 0 {
		return true
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

// iptablesVerdicts are the targets that end a packet's walk, or return
// from a chain; any other target that is not a chain (LOG, MARK) lets the
// packet go on.
var iptablesVerdicts = map[string]string{"ACCEPT": "accept", "DROP": "drop", "REJECT": "reject", "DNAT": "dnat", "RETURN": "return"}

// parseIptablesSave reads iptables-save: "*table", ":CHAIN POLICY [n:m]"
// ("-" for a chain of the user's), "-A CHAIN matches -j TARGET".
func parseIptablesSave(text string) *Firewall {
	fw := &Firewall{Format: "iptables-save", Chains: map[string]*fwChain{}}
	table := ""
	type pending struct {
		table, chain string
		rule         fwRule
	}
	var rules []pending
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		f := strings.Fields(line)
		switch {
		case strings.HasPrefix(line, "*"):
			table = line[1:]
		case strings.HasPrefix(line, ":") && len(f) >= 2:
			name := table + "/" + f[0][1:]
			fw.Chains[name] = &fwChain{Name: f[0][1:], Accept: f[1] != "DROP"}
			if table == "filter" && f[0] == ":INPUT" {
				fw.Inputs = append(fw.Inputs, name)
			}
		case strings.HasPrefix(line, "-A ") && len(f) >= 2:
			r := iptablesRule(f[2:])
			r.Line, r.Text = n+1, line
			rules = append(rules, pending{table, table + "/" + f[1], r})
		}
	}
	// A target is a chain only once every chain is declared: iptables-save
	// prints the declarations of a table before its rules, but a reader
	// should not depend on it.
	for i := range rules {
		p := &rules[i]
		r := p.rule
		if r.Target != "" {
			verdict, isVerdict := iptablesVerdicts[r.Target]
			_, isChain := fw.Chains[p.table+"/"+r.Target]
			switch {
			case isVerdict:
				r.Verdict, r.Target = verdict, ""
			case isChain:
				r.Target = p.table + "/" + r.Target
			default:
				r.Verdict, r.Target = "", "" // LOG, MARK, CT: the packet goes on
			}
		}
		fw.Rules = append(fw.Rules, r)
		if c, ok := fw.Chains[p.chain]; ok {
			c.Rules = append(c.Rules, r)
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
		case "-o", "--out-interface":
			i++
		case "-m", "--match":
			i++ // the module names nothing; its options do
		case "-j", "--jump":
			r.Verdict, r.Target, i = "jump", arg, i+1
		case "-g", "--goto":
			r.Verdict, r.Target, i = "goto", arg, i+1
		case "--to-destination":
			r.ToPort = hostPort(arg)
			i++
		default:
			if strings.HasPrefix(f[i], "-") {
				// --ctstate, --mark, --dst-type, --limit: a condition this
				// reader does not interpret. Its value is skipped with it.
				r.Other = true
				if arg != "" && !strings.HasPrefix(arg, "-") {
					i++
				}
			}
		}
		negate = false
	}
	return r
}

// parseNft reads nft list ruleset: tables, chains in braces, a chain's
// "type … hook input …; policy …;" line, and one rule a line.
func parseNft(text string) *Firewall {
	fw := &Firewall{Format: "nft", Chains: map[string]*fwChain{}}
	var chain *fwChain
	key, table := "", ""
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		f := strings.Fields(line)
		switch {
		case len(f) == 0:
		case f[0] == "table" && len(f) >= 3:
			table = f[1] + " " + f[2]
		case f[0] == "chain" && len(f) >= 2:
			key = table + "/" + f[1]
			chain = &fwChain{Name: f[1], Accept: true}
			fw.Chains[key] = chain
			if strings.HasSuffix(line, "}") { // an empty chain on one line
				chain = nil
			}
		case f[0] == "}":
			chain = nil
		case chain == nil:
		case f[0] == "type":
			if strings.Contains(line, "hook input") {
				fw.Inputs = append(fw.Inputs, key)
			}
			if strings.Contains(line, "policy drop") {
				chain.Accept = false
			}
		case f[0] == "policy":
			chain.Accept = !strings.HasPrefix(f[1], "drop")
		default:
			r := nftRule(f)
			if r.Target != "" {
				r.Target = table + "/" + r.Target
			}
			r.Line, r.Text = n+1, line
			fw.Rules = append(fw.Rules, r)
			chain.Rules = append(chain.Rules, r)
		}
	}
	return fw
}

// nftConditions are the words of a rule this reader interprets, or that
// change nothing for the question; a rule holding any other is taken as
// matching on something unread.
var nftConditions = map[string]bool{
	"tcp": true, "udp": true, "th": true, "dport": true, "ip": true, "ip6": true, "saddr": true, "daddr": true,
	"iifname": true, "iif": true, "oifname": true, "meta": true, "l4proto": true, "counter": true,
	"packets": true, "bytes": true, "accept": true, "drop": true, "reject": true, "with": true,
	"icmpx": true, "icmp": true, "type": true, "admin-prohibited": true, "port-unreachable": true,
	"jump": true, "goto": true, "return": true, "dnat": true, "to": true, "log": true, "prefix": true,
	"comment": true, "!=": true, "==": true, "{": true, "}": true, ",": true,
}

func nftRule(f []string) fwRule {
	var r fwRule
	for i := 0; i < len(f); i++ {
		next := ""
		if i+1 < len(f) {
			next = f[i+1]
		}
		switch w := f[i]; w {
		case "tcp", "udp", "th":
			if next == "dport" && i+2 < len(f) {
				if w != "th" {
					r.Proto = w
				}
				set, used := nftValue(f[i+2:])
				r.Ports, i = portList(set, ",", "-"), i+1+used
			}
		case "saddr":
			r.Limited = r.Limited || next != "!="
			i++
		case "daddr":
			i++
		case "iifname", "iif":
			r.Limited = r.Limited || strings.Trim(next, `"`) == "lo"
			i++
		case "oifname":
			i++
		case "l4proto":
			r.Proto = next
			if next == "{" {
				r.Proto = "set"
			}
			i++
		case "accept", "drop", "reject", "return":
			r.Verdict = w
		case "jump", "goto":
			r.Verdict, r.Target = w, next
			i++
		case "dnat":
			r.Verdict = "dnat"
			for _, t := range f[i+1:] {
				if p := hostPort(t); p > 0 {
					r.ToPort = p
				}
			}
			return r
		case "comment", "prefix":
			return r // the rest is a string
		default:
			if !nftConditions[w] && !isNumberish(w) && !strings.HasPrefix(w, `"`) {
				r.Other = true // ct, mark, fib, a set lookup: unread
			}
		}
	}
	return r
}

// isNumberish says whether a word is a value — a number, an address, a
// list — rather than a keyword.
func isNumberish(w string) bool {
	w = strings.Trim(w, ",{}")
	return w == "" || strings.ContainsAny(w[:1], "0123456789:")
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

// walk is one chain's answer for the packet: accepted (with the rule that
// did it), dropped, or neither — fallen through, or returned.
type walk struct {
	decided, accept bool
	why             string
	unknown         []string // the jumps that could not be followed
}

// maxJumps bounds the depth of the jumps followed: past it, a loop.
const maxJumps = 16

// chain walks the packet through the chain named key.
func (fw *Firewall) chain(key string, port, depth int) walk {
	c, ok := fw.Chains[key]
	if !ok {
		return walk{unknown: []string{"a jump to " + key + ", which the ruleset does not define"}}
	}
	if depth > maxJumps {
		return walk{unknown: []string{fmt.Sprintf("jumps deeper than %d from %s: a loop", maxJumps, key)}}
	}
	var unknown []string
	for i := range c.Rules {
		r := &c.Rules[i]
		if !r.applies(port) || r.Verdict == "" || r.Verdict == "dnat" {
			continue
		}
		where := fmt.Sprintf("%s line %d: %s", fw.Path, r.Line, r.Text)
		switch r.Verdict {
		case "jump", "goto":
			sub := fw.chain(r.Target, port, depth+1)
			unknown = append(unknown, sub.unknown...)
			if r.Other {
				// Whether the packet takes this jump is not read: what the
				// chain would do only counts if it would let it in.
				if sub.decided && sub.accept {
					unknown = append(unknown, "accepted behind a condition not read, "+where)
				}
				continue
			}
			if sub.decided {
				return walk{decided: true, accept: sub.accept, why: sub.why, unknown: unknown}
			}
			if r.Verdict == "goto" {
				return walk{unknown: unknown} // back to the caller of this chain
			}
		case "return":
			if !r.Other {
				return walk{unknown: unknown}
			}
		default:
			if r.Other {
				continue
			}
			return walk{decided: true, accept: r.Verdict == "accept", why: "accepted from anywhere, " + where, unknown: unknown}
		}
	}
	return walk{unknown: unknown}
}

// exposed says how a new connection from anywhere reaches port, empty when
// none does, and what the walk could not follow: a DNAT that publishes it —
// Docker's -p, which never crosses INPUT — or every input chain letting it
// through, by a rule or by its policy.
func (fw *Firewall) exposed(port int) (how string, unknown []string) {
	for i := range fw.Rules {
		r := &fw.Rules[i]
		if r.Verdict == "dnat" && !r.Limited && !r.Other && (r.applies(port) && len(r.Ports) > 0 || (r.ToPort == port && len(r.Ports) > 0)) {
			return fmt.Sprintf("published by a DNAT, %s line %d: %s", fw.Path, r.Line, r.Text), nil
		}
	}
	if len(fw.Inputs) == 0 {
		return "no input chain filters it in " + fw.Path, nil
	}
	var why []string
	for _, key := range fw.Inputs {
		w := fw.chain(key, port, 0)
		unknown = append(unknown, w.unknown...)
		switch {
		case w.decided && !w.accept:
			return "", unknown
		case w.decided:
			why = append(why, w.why)
		case !fw.Chains[key].Accept:
			return "", unknown
		default:
			why = append(why, "let through by the policy of chain "+fw.Chains[key].Name+", which accepts")
		}
	}
	return strings.Join(why, "; "), unknown
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
			if n, ok := t.Postgres.integer("port"); ok && n > 0 && n <= 65535 {
				port = int(n)
			}
			how, unknown := t.Firewall.exposed(port)
			proposed := "accept the port from the application's network alone (a source address on the rule), or publish it on 127.0.0.1"
			why := "a database port open to every address is tried by every scanner on the " +
				"network; pg_hba.conf then stands alone between them and the data, and a " +
				"trust or md5 line there becomes the whole defence"
			switch {
			case how != "":
				return []Finding{{
					Current:  fmt.Sprintf("PostgreSQL's port %d reachable from any address: %s", port, how),
					Proposed: proposed,
					Why:      why,
				}}
			case len(unknown) > 0:
				return []Finding{{
					Current:  fmt.Sprintf("whether PostgreSQL's port %d is reachable from any address is not known: %s", port, strings.Join(unknown, "; ")),
					Proposed: "check the chains named by hand, or save the ruleset whole (nft list ruleset, iptables-save without a table filter)",
					Verdict:  VerdictUndetermined,
					Why:      "a jump the audit cannot follow may hold the rule that opens the port: " + why,
				}}
			}
			return nil
		},
	})
}
