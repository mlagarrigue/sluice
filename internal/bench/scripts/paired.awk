# Per-round A/B ratio, judged by a sign test on the paired rounds.
#
# Pairing kills drift: each round runs both binaries back to back, so a clock
# ramp or a Windows task that slows the whole round cancels out of the ratio.
# The sign test then asks the only question that matters — did "after" win in
# enough rounds that chance is not a credible explanation — which needs no
# assumption about the noise being normal, and one wild round cannot swing it.
{ v[$3 SUBSEP $2 SUBSEP $1] = $4; name[$3]; round[$2] }
END {
  for (nm in name) {
    n = 0; wins = 0; losses = 0
    for (r in round) {
      if (r == 1) continue            # first round absorbs the clock ramp
      b = v[nm SUBSEP r SUBSEP "before"]; a = v[nm SUBSEP r SUBSEP "after"]
      if (b == "" || a == "" || b+0 == 0) continue
      ratio[++n] = (a+0)/(b+0); B[n] = b+0; A[n] = a+0
      # A tie is evidence for neither side, so the sign test drops it rather
      # than scoring it a loss: counted as one, ten identical rounds out of
      # eleven read as "SLOWER" and a real speedup diluted by ties read as noise.
      if (a+0 < b+0) wins++; else if (a+0 > b+0) losses++
    }
    # Below ten paired rounds the sign test is too easy to satisfy by accident:
    # an untouched benchmark measured "faster -8.1%" at n=8 on this machine.
    if (n < 10) { printf "%-42s need >=11 rounds, got n=%d\n", nm, n; continue }
    for (i=1;i<=n;i++) for (j=i+1;j<=n;j++) {
      if (ratio[i]>ratio[j]) { t=ratio[i];ratio[i]=ratio[j];ratio[j]=t }
      if (B[i]>B[j]) { t=B[i];B[i]=B[j];B[j]=t }
      if (A[i]>A[j]) { t=A[i];A[i]=A[j];A[j]=t }
    }
    med = (n%2)?ratio[(n+1)/2]:(ratio[n/2]+ratio[n/2+1])/2
    mb  = (n%2)?B[(n+1)/2]:(B[n/2]+B[n/2+1])/2
    ma  = (n%2)?A[(n+1)/2]:(A[n/2]+A[n/2+1])/2
    # Sign test at the two-sided 5% level, i.e. a one-sided tail of 2.5% in
    # each direction: the smallest k with P(X >= k | n, p = 1/2) <= 0.025,
    # computed exactly from the binomial tail rather than read off a table.
    # The table this replaced was the 5% tail (two-sided 10%): at n = 11,
    # which is what the gate runs, it accepted 9 wins, P = 67/2048 = 0.033.
    # In doubles the tail is exact well past any round count anyone runs.
    # The trial count is the untied rounds only, m, as the sign test defines it.
    m = wins + losses
    need = m + 1
    for (k = m; k >= 0; k--) {
      tail = 0; c = 1
      for (j = 0; j <= m; j++) { if (j >= k) tail += c; c = c * (m - j) / (j + 1) }
      if (tail / 2^m <= 0.025) need = k; else break
    }
    decided = (wins >= need) ? "faster" : ((losses >= need) ? "SLOWER" : "")

    # An effect floor, and it is not decoration. The sign test controls the
    # error rate of ONE comparison at ~5%; a gate running twenty of them fails
    # on identical binaries about two times in three, which is what a null run
    # against HEAD demonstrated — "SLOWER +0.6%" and "SLOWER +0.2%" on two
    # copies of the same code. A gate that cries wolf is a gate nobody reads
    # after a month.
    #
    # Requiring a minimum median ratio fixes it where the statistics cannot:
    # the sign test says the direction is real, the floor says the size is
    # worth acting on. Set MINEFFECT to 0 to see every verdict the test
    # produces, which is what an investigation wants and a gate does not.
    effect = (med > 1) ? med - 1 : 1 - med
    if (decided != "" && MINEFFECT > 0 && effect < MINEFFECT) {
      verdict = sprintf("noise (%+.1f%%, below the %.0f%% floor)", 100*(med-1), 100*MINEFFECT)
    } else if (decided == "") {
      verdict = "noise"
    } else {
      verdict = sprintf("%s %+.1f%%", decided, 100*(med-1))
    }
    ties = (n > m) ? sprintf(" ties=%d", n - m) : ""
    printf "%-42s %8.4f -> %8.4f  %-16s wins=%d/%d (need %d)%s\n", nm, mb, ma, verdict, wins, m, need, ties
  }
}
