package main

// leftover_passwords.go — no password is kept as plain text.
//
// Long ago, passwords were stored as typed, readable by anyone who could
// read the database (a backup, a leaked copy, a support query). Then they
// started being scrambled (bcrypt), and an old plain one was scrambled the
// next time its owner signed in (IsValidUser).
//
// That left every account nobody had signed in to since with its password
// readable, for ever. People reuse passwords, so one leaked copy of the
// database would have handed out their email passwords too.
//
// So at every start this scrambles whatever is left, in the background,
// without signing anyone out: the password itself does not change, only
// how it is kept. Sign-in keeps the old path for a row an older server
// writes in the middle of a deploy.

import "log"

// leftoverBatch is how many plain passwords are scrambled per round. Each
// takes about a tenth of a second, so a round is about twenty seconds.
const leftoverBatch = 200

// scramble is how this job scrambles a password: hashPassword, the same
// as every other. A variable only so its test can use bcrypt's fastest
// setting; at full strength, under the race detector, it took half a
// minute of CI's ten.
var scramble = hashPassword

// hashLeftoverPasswords scrambles every password still kept as plain text,
// and clears plain ones that sit next to a scrambled one (sign-in never
// reads those; they only sit there, readable). It returns how many it
// scrambled and how many it cleared.
func hashLeftoverPasswords() (hashed, cleared int) {
	res, err := db.Exec(`
		UPDATE users SET password = ''
		 WHERE COALESCE(password, '') <> '' AND COALESCE(password_hash, '') <> ''`)
	if err != nil {
		log.Printf("passwords: could not clear plain passwords kept beside scrambled ones (%v); they stay readable until the next start", err)
	} else if n, nerr := res.RowsAffected(); nerr == nil {
		cleared = int(n)
	}

	for {
		type leftover struct {
			id    int
			plain string
		}
		rows, err := db.Query(`
			SELECT id, password FROM users
			 WHERE COALESCE(password, '') <> '' AND COALESCE(password_hash, '') = ''
			 ORDER BY id LIMIT $1`, leftoverBatch)
		if err != nil {
			log.Printf("passwords: could not look for passwords kept as plain text (%v); they stay readable until the next start", err)
			break
		}
		var batch []leftover
		bad := 0
		for rows.Next() {
			var l leftover
			if err := rows.Scan(&l.id, &l.plain); err != nil {
				bad++
				continue
			}
			batch = append(batch, l)
		}
		rerr := rows.Err()
		rows.Close()
		if bad > 0 || rerr != nil {
			log.Printf("passwords: %d rows of plain passwords could not be read (%v); those stay readable until the next start", bad, rerr)
		}
		if len(batch) == 0 {
			break
		}

		done := 0
		for _, l := range batch {
			h, err := scramble(l.plain)
			if err != nil {
				log.Printf("passwords: could not scramble account %d's password (%v); it stays readable until the next start", l.id, err)
				continue
			}
			// Only if nobody changed it meanwhile: a sign-in can scramble
			// the same row at the same moment, and its answer wins.
			res, err := db.Exec(`
				UPDATE users SET password_hash = $1, password = ''
				 WHERE id = $2 AND password = $3 AND COALESCE(password_hash, '') = ''`,
				h, l.id, l.plain)
			if err != nil {
				log.Printf("passwords: could not save account %d's scrambled password (%v); it stays readable until the next start", l.id, err)
				continue
			}
			if n, _ := res.RowsAffected(); n == 1 {
				hashed++
			}
			done++
		}
		// A round in which nothing could be saved would come round again
		// with the same rows, for ever. Stop, and try at the next start.
		if done == 0 {
			break
		}
	}

	if hashed > 0 || cleared > 0 {
		log.Printf("passwords: %d kept as plain text are now scrambled, and %d plain copies beside scrambled ones are cleared; nobody was signed out", hashed, cleared)
	}
	return hashed, cleared
}
