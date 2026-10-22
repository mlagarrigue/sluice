package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// The connector meets each authentication method a pg_hba.conf can impose,
// on a real server, and does what its documentation says of each:
//
//   - md5 is declined, ErrAuthUnsupported, with the remedy in the message;
//   - password (the credential asked in clear) is refused unless
//     Config.AllowCleartextPassword is set, and then succeeds;
//   - trust connects with no credential at all, and is refused under
//     Config.RequireAuth.
//
// scripts/pg-hba.sh puts one method in front of each of three roles, which
// this test creates; SLUICE_PG_HBA=1 says it ran. The md5 role's password is
// stored as an md5 hash, or the server would answer the md5 line with SCRAM
// and the case would test nothing.
func TestIntegrationAuthMethods(t *testing.T) {
	addr, cfg := pgTarget(t)
	if os.Getenv("SLUICE_PG_HBA") != "1" {
		t.Skip("SLUICE_PG_HBA is not set: scripts/pg-hba.sh has not put the methods in front of the server")
	}
	const password = "hba-pass" //nolint:gosec // G101: throwaway roles' password
	admin := dial(t)
	for _, r := range []struct{ role, encryption string }{
		{"sluice_hba_md5", "md5"},
		{"sluice_hba_password", "scram-sha-256"},
		{"sluice_hba_trust", "scram-sha-256"},
	} {
		drop := func(ctx context.Context) error { return execContext(ctx, admin, "DROP ROLE IF EXISTS "+r.role) }
		if err := drop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := exec(t, admin, "SET password_encryption = '"+r.encryption+"'"); err != nil {
			t.Fatal(err)
		}
		if err := exec(t, admin, "CREATE ROLE "+r.role+" LOGIN PASSWORD '"+password+"'"); err != nil {
			t.Fatalf("create role %s: %v", r.role, err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := drop(ctx); err != nil {
				t.Errorf("drop role %s: %v", r.role, err)
			}
		})
	}
	if err := exec(t, admin, "RESET password_encryption"); err != nil {
		t.Fatal(err)
	}

	connect := func(role, pw string, adjust func(*postgres.Config)) error {
		c := cfg
		c.User, c.Password = role, pw
		if adjust != nil {
			adjust(&c)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), c)
		if err == nil {
			_ = conn.Close()
		}
		return err
	}

	t.Run("md5", func(t *testing.T) {
		err := connect("sluice_hba_md5", password, nil)
		if !errors.Is(err, postgres.ErrAuthUnsupported) || !strings.Contains(err.Error(), "md5") {
			t.Fatalf("md5 answered with %v, want ErrAuthUnsupported naming md5", err)
		}
	})
	t.Run("password", func(t *testing.T) {
		err := connect("sluice_hba_password", password, nil)
		if !errors.Is(err, postgres.ErrAuth) || !strings.Contains(err.Error(), "AllowCleartextPassword") {
			t.Fatalf("a cleartext request answered with %v, want ErrAuth naming AllowCleartextPassword", err)
		}
		if err := connect("sluice_hba_password", password, func(c *postgres.Config) { c.AllowCleartextPassword = true }); err != nil {
			t.Fatalf("with AllowCleartextPassword: %v", err)
		}
		err = connect("sluice_hba_password", password+"-wrong", func(c *postgres.Config) { c.AllowCleartextPassword = true })
		var pgErr *postgres.Error
		if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
			t.Fatalf("a wrong cleartext password answered with %v, want the server's 28P01", err)
		}
	})
	t.Run("trust", func(t *testing.T) {
		if err := connect("sluice_hba_trust", "", nil); err != nil {
			t.Fatalf("trust with no password: %v", err)
		}
		err := connect("sluice_hba_trust", password, func(c *postgres.Config) { c.RequireAuth = true })
		if !errors.Is(err, postgres.ErrAuth) || !strings.Contains(err.Error(), "RequireAuth") {
			t.Fatalf("trust under RequireAuth answered with %v, want ErrAuth naming RequireAuth", err)
		}
	})
}
