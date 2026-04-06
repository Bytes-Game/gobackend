package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

// db is the PostgreSQL connection pool shared across the application.
var db *sql.DB

// ---------------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------------

// InitDatabase connects to PostgreSQL, runs schema migrations, and seeds
// sample data if the tables are empty.
func InitDatabase() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	runMigrations()
	seedIfEmpty()
}

// runMigrations creates all required tables idempotently.
func runMigrations() {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id          SERIAL PRIMARY KEY,
		username	VARCHAR(50) UNIQUE NOT NULL,
		password	VARCHAR(255) NOT NULL,
		full_name	VARCHAR(100) DEFAULT '',
		wins		INT DEFAULT 0,
		losses		INT DEFAULT 0,
		league		VARCHAR(20) DEFAULT 'Bronze'
	);

	CREATE TABLE IF NOT EXISTS follows (
		follower_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		following_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at	 TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (follower_id, following_id)
	);

	CREATE TABLE IF NOT EXISTS posts (
		id			  SERIAL PRIMARY KEY,
		author_id	  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		type		  VARCHAR(10) NOT NULL DEFAULT 'image',
		content_url   TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		caption		  TEXT DEFAULT '',
		views		  INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS post_likes (
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		user_id	   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (post_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS comments(
		id		   SERIAL PRIMARY KEY,
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		author_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		text	   TEXT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenges(
		id 		  	  SERIAL PRIMARY KEY,
		creator_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		prefix 		  VARCHAR(100) NOT NULL DEFAULT '',
		subject       VARCHAR(100) NOT NULL DEFAULT '',
		visibility    VARCHAR(10) NOT NULL DEFAULT 'arena',
		status        VARCHAR(20) NOT NULL DEFAULT 'open',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_visible_to (
		challenge_id INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		PRIMARY KEY (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_responses (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		responder_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_likes (
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY  (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_response_likes (
		response_id   INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (response_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_votes (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		response_id   INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		voter_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		UNIQUE (challenge_id, voter_id)
	);

	CREATE TABLE IF NOT EXISTS watch_events (
		id            SERIAL PRIMARY KEY,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		content_id    INT NOT NULL,
		content_type  VARCHAR(20) NOT NULL,
		watch_time    INT NOT NULL DEFAULT 0,
		completed     BOOLEAN DEFAULT FALSE,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS reports (
		id            SERIAL PRIMARY KEY,
		reporter_id   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		target_id     INT NOT NULL,
		target_type   VARCHAR(20) NOT NULL,
		reason        VARCHAR(100) NOT NULL,
		description   TEXT DEFAULT '',
		status        VARCHAR(20) NOT NULL DEFAULT 'pending',
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS chat_messages (
		id            SERIAL PRIMARY KEY,
		sender_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		receiver_id   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		message       TEXT NOT NULL,
		is_read       BOOLEAN DEFAULT FALSE,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_comments (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		author_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		text          TEXT NOT NULL,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS saved_challenges (
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (user_id, challenge_id)
	);
	`

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// Add new columns to existing tables safely
	alterStmts := `
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN status VARCHAR(20) DEFAULT 'sent'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN reply_to_id INT REFERENCES chat_messages(id) ON DELETE SET NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_edited BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN edited_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN last_seen TIMESTAMPTZ DEFAULT NOW(); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	`
	if _, err := db.Exec(alterStmts); err != nil {
		log.Printf("Warning: alter table issue: %v", err)
	}

	// Performance indexes
	indexes := `
	CREATE INDEX IF NOT EXISTS idx_posts_author_id ON posts(author_id);
	CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_follows_follower_id ON follows(follower_id);
	CREATE INDEX IF NOT EXISTS idx_follows_following_id ON follows(following_id);
	CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
	CREATE INDEX IF NOT EXISTS idx_post_likes_post_id ON post_likes(post_id);
	CREATE INDEX IF NOT EXISTS idx_challenges_visibility ON challenges(visibility);
	CREATE INDEX IF NOT EXISTS idx_challenges_status ON challenges(status);
	CREATE INDEX IF NOT EXISTS idx_challenges_created_at ON challenges(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_challenge_responses_challenge_id ON challenge_responses(challenge_id);
	CREATE INDEX IF NOT EXISTS idx_challenge_votes_challenge_id ON challenge_votes(challenge_id);
	CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events(user_id);
	CREATE INDEX IF NOT EXISTS idx_watch_events_content ON watch_events(content_id, content_type);
	CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);
	CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_sender ON chat_messages(sender_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_receiver ON chat_messages(receiver_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_pair ON chat_messages(sender_id, receiver_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_challenge_comments_challenge_id ON challenge_comments(challenge_id, created_at ASC);
	CREATE INDEX IF NOT EXISTS idx_saved_challenges_user ON saved_challenges(user_id, created_at DESC);
	`
	if _, err := db.Exec(indexes); err != nil {
		log.Printf("Warning: index creation issue: %v", err)
	}

	log.Println("Database migrations completed")
}

// --------------------------------------------------------------------------
// User CRUD
// --------------------------------------------------------------------------

// readUser scans basic user columns and enriches with follower count + following list
// Use for single-user lookups (3 queries total).
func readUser(id int, username, pw, fullName string, wins, losses int, league string) User {
	u := User{
		ID:       strconv.Itoa(id),
		Username: username,
		password: pw,
		FullName: fullName,
		Wins:     wins,
		Losses:   losses,
		League:   league,
	}

	// Followers count
	_ = db.QueryRow(`SELECT COUNT(*) FROM follows WHERE following_id = $1`, id).Scan(&u.Followers)

	// Following list (string IDs for JSON compatibility)
	rows, err := db.Query(`SELECT following_id FROM follows WHERE follower_id = $1`, id)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var fid int
			if rows.Scan(&fid) == nil {
				u.FollowingList = append(u.FollowingList, strconv.Itoa(fid))
			}
		}
	}
	if u.FollowingList == nil {
		u.FollowingList = []string{}
	}
	return u
}

// enrichUsers batch-populates Followers and FollowingList for a slice of users
// using only one extra DB query (much faster for lists).
func enrichUsers(users []User) {
	if len(users) == 0 {
		return
	}

	rows, err := db.Query(`SELECT follower_id, following_id FROM follows`)
	if err != nil {
		return
	}
	defer rows.Close()

	followingMap := make(map[string][]string) // follower → []following
	followerCount := make(map[string]int)     // user → count of followers

	for rows.Next() {
		var fid, tid int
		if rows.Scan(&fid, &tid) == nil {
			fidStr := strconv.Itoa(fid)
			tidStr := strconv.Itoa(tid)
			followingMap[fidStr] = append(followingMap[fidStr], tidStr)
			followerCount[tidStr]++
		}
	}

	for i := range users {
		users[i].Followers = followerCount[users[i].ID]
		users[i].FollowingList = followingMap[users[i].ID]
		if users[i].FollowingList == nil {
			users[i].FollowingList = []string{}
		}
	}
}

// GetuserByUsername returns a fully enriched user, looked up by username.
func GetUserByUsername(username string) (User, bool) {
	var id, wins, losses int
	var uname, pw, fullName, league string
	err := db.QueryRow(
		`SELECT id, username, password, full_name, wins, losses, league FROM users WHERE username = $1`,
		username,
	).Scan(&id, &uname, &pw, &fullName, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(id, uname, pw, fullName, wins, losses, league), true
}

// GetUserByID returns a fully enriched user, looked up by string ID.
func GetUserByID(idStr string) (User, bool) {
	idInt, err := strconv.Atoi(idStr)
	if err != nil {
		return User{}, false
	}
	var wins, losses int
	var uname, pw, fullName, league string
	err = db.QueryRow(
		`SELECT id, username, password, full_name, wins, losses, league FROM users WHERE id = $1`,
		idInt,
	).Scan(&idInt, &uname, &pw, &fullName, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(idInt, uname, pw, fullName, wins, losses, league), true
}

// UserExists checks whether a username is already taken.
func UserExists(username string) bool {
	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1`, username).Scan(&exists)
	return exists
}

// IsValidUser checks credentials (plain-text comparison - hash in production).
func IsValidUser(username, password string) bool {
	var exists bool
	db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1 AND password = $2)`,
		username, password,
	).Scan(&exists)
	return exists
}

// GetAllUsers returns every user, batch-enriched with follow data.
func GetAllUsers() []User {
	rows, err := db.Query(`SELECT id, username, password, full_name, wins, losses, league FROM users ORDER BY id`)
	if err != nil {
		log.Printf("GetAllUsers error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var id, wins, losses int
		var uname, pw, fullName, league string
		if rows.Scan(&id, &uname, &pw, &fullName, &wins, &losses, &league) == nil {
			result = append(result, User{
				ID:       strconv.Itoa(id),
				Username: uname,
				password: pw,
				FullName: fullName,
				Wins:     wins,
				Losses:   losses,
				League:   league,
			})
		}
	}

	enrichUsers(result)
	return result
}

// --------------------------------------------------------------------------------
// Post CRUD
// --------------------------------------------------------------------------------

// postBaseQuery is the common SELECT used for all post retrievals.
// It joins author info and aggregates likes + comments in a single scan.
const postBaseQuery = `
SELECT p.id, p.author_id, u.username, u.league,
	   p.type,
	   COALESCE(p.content_url,   '') AS content_url,
	   COALESCE(p.thumbnail_url, '') AS thumbnail_url,
	   p.caption, p.views,
	   COALESCE(lc.cnt, 0) AS likes,
	   COALESCE(cc.cnt, 0) AS comment_count,
	   p.created_at
FROM posts p
JOIN users u ON p.author_id = u.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM post_likes GROUP BY post_id) lc ON lc.post_id = p.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM comments GROUP BY post_id) cc ON cc.post_id = p.id`

// queryPosts executes a full post query string and returns enriched Post structs
// including the LikedBy list (batch-fetched).
func queryPosts(query string, args ...interface{}) []Post {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryPosts error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Post
	var postIDs []int

	for rows.Next() {
		var id, authorID, views, likes, comments int
		var username, league, postType, contentURL, thumbnailURL, caption string
		var createdAt time.Time

		if rows.Scan(&id, &authorID, &username, &league,
			&postType, &contentURL, &thumbnailURL,
			&caption, &views, &likes, &comments, &createdAt) == nil {

			result = append(result, Post{
				ID:             strconv.Itoa(id),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				AuthorLeague:   league,
				Type:           postType,
				ContentURL:     contentURL,
				ThumbnailURL:   thumbnailURL,
				Caption:        caption,
				Views:          views,
				Likes:          likes,
				Comments:       comments,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
			postIDs = append(postIDs, id)
		}
	}

	// Batch-fetch LikedBy for every returned post (1 extra query)
	if len(postIDs) > 0 {
		likedByMap := getLikedByMap(postIDs)
		for i := range result {
			if lb, ok := likedByMap[result[i].ID]; ok {
				result[i].LikedBy = lb
			}
		}
	}

	return result
}

// getLikedByMap returns a map[postip==IDStr] -> []userIDStr for the given post IDs.
func getLikedByMap(postIDs []int) map[string][]string {
	result := make(map[string][]string)
	if len(postIDs) == 0 {
		return result
	}

	placeholders := ""
	args := make([]interface{}, len(postIDs))
	for i, id := range postIDs {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	rows, err := db.Query(
		fmt.Sprintf(`SELECT post_id, user_id FROM post_likes WHERE post_id IN (%s)`, placeholders),
		args...,
	)
	if err != nil {
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var pid, uid int
		if rows.Scan(&pid, &uid) == nil {
			pidStr := strconv.Itoa(pid)
			result[pidStr] = append(result[pidStr], strconv.Itoa(uid))
		}
	}
	return result
}

// GetAllPosts returns every post, newest first.
func GetAllPosts() []Post {
	return queryPosts(postBaseQuery + ` ORDER BY p.created_at DESC`)
}

// GetPostsPaginated returns a page of posts and whether more pages exist.
func GetPostsPaginated(page, limit int) ([]Post, bool) {
	offset := (page - 1) * limit
	// Fetch one extra row to check if there's a next page
	posts := queryPosts(postBaseQuery+` ORDER BY p.created_at DESC LIMIT $1 OFFSET $2`, limit+1, offset)
	hasMore := len(posts) > limit
	if hasMore {
		posts = posts[:limit]
	}
	return posts, hasMore
}

// GetPostsByUserID returns all posts by a given author.
func GetPostsByUserID(userID string) []Post {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	return queryPosts(postBaseQuery+` WHERE p.author_id = $1 ORDER BY p.created_at DESC`, uid)
}

// GetPostByID returns a single post.
func GetPostByID(idStr string) (Post, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Post{}, false
	}
	posts := queryPosts(postBaseQuery+` WHERE p.id = $1`, id)
	if len(posts) == 0 {
		return Post{}, false
	}
	return posts[0], true
}

// ToggleLike adds or removes a like and returns (liked, newCount, UpdatedPost).
func ToggleLike(postID, userID string) (bool, int, Post) {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(userID)
	if err1 != nil || err2 != nil {
		return false, 0, Post{}
	}

	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM post_likes WHERE post_id=$1 AND user_id=$2)`, pid, uid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM post_likes WHERE post_id=$1 AND user_id=$2`, pid, uid)
	} else {
		db.Exec(`INSERT INTO post_likes (post_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, pid, uid)
	}

	post, found := GetPostByID(postID)
	if !found {
		return false, 0, Post{}
	}
	return !exists, post.Likes, post
}

// ---------------------------------------------------------------------------
// Comment CRUD
// ---------------------------------------------------------------------------

// AddComment inserts a new comment and returns the created Comment struct.
func AddComment(postID, authorID, authorUsername, text string) Comment {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(authorID)
	if err1 != nil || err2 != nil {
		return Comment{}
	}

	var id int
	var createdAt time.Time
	err := db.QueryRow(
		`INSERT INTO comments (post_id, author_id, text) VALUES ($1,$2,$3) RETURNING id, created_at`,
		pid, uid, text,
	).Scan(&id, &createdAt)
	if err != nil {
		log.Printf("AddComment error: %v", err)
		return Comment{}
	}

	return Comment{
		ID:             strconv.Itoa(id),
		PostID:         postID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
	}
}

// GetComments returns all comments for a post, with author usernames resolved via JOIN
func GetComments(postID string) []Comment {
	pid, err := strconv.Atoi(postID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT c.id, c.post_id, c.author_id, u.username, c.text, c.created_at
		FROM comments c
		JOIN users u ON c.author_id = u.id
		WHERE c.post_id = $1
		ORDER BY c.created_at ASC`, pid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Comment
	for rows.Next() {
		var cid, postIDInt, authorID int
		var username, txt string
		var createdAt time.Time
		if rows.Scan(&cid, &postIDInt, &authorID, &username, &txt, &createdAt) == nil {
			result = append(result, Comment{
				ID:             strconv.Itoa(cid),
				PostID:         strconv.Itoa(postIDInt),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				Text:           txt,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// -----------------------------------------------------------------------------
// Seed data (runs once when tables are empty)
// -----------------------------------------------------------------------------

func seedIfEmpty() {
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	if count > 0 {
		log.Println("Database already has data - skipping seed")
		return
	}

	log.Println("Seeding database with initial data...")
	seedUsers()
	seedFollows()
	seedPosts()
	seedComments()
	seedChallenges()
	log.Println("Seeding complete")
}

// ReseedDatabase drops all data and re-seeds from scratch.
func ReseedDatabase() {
	log.Println("Reseeding database...")
	// TRUNCATE with RESTART IDENTITY resets all SERIAL counters to 1
	db.Exec(`TRUNCATE TABLE
		chat_messages,
		challenge_votes, challenge_response_likes, challenge_likes, challenge_responses,
		challenge_visible_to, challenges,
		comments, post_likes, posts, follows, watch_events, reports,
		users
		RESTART IDENTITY CASCADE`)
	seedUsers()
	seedFollows()
	seedPosts()
	seedComments()
	seedChallenges()
	log.Println("Reseed complete")
}

func seedUsers() {
	type su struct {
		username, password, fullName, league string
		wins, losses                         int
	}
	data := []su{
		{"player1", "pass1", "Player One", "Gold", 32, 18},
		{"player2", "pass2", "Player Two", "Diamond", 120, 45},
		{"player3", "pass3", "Player Three", "Bronze", 10, 5},
		{"shadowstrike", "pass4", "Shadow Strike", "Platinum", 95, 30},
		{"blazerunner", "pass5", "Blaze Runner", "Silver", 55, 40},
		{"stormchaser", "pass6", "Storm Chaser", "Gold", 78, 22},
		{"frostbyte", "pass7", "Frost Byte", "Dianond", 140, 50},
		{"nightowl", "pass8", "Night Owl", "Bronze", 15, 10},
		{"thunderbolt", "pass9", "Thunder Bolt", "Silver", 62, 38},
		{"cyberking", "pass10", "Cyber King", "Platinum", 100, 25},
	}
	for _, u := range data {
		db.Exec(
			`INSERT INTO users (username, password, full_name, wins, losses, league)
			 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
			u.username, u.password, u.fullName, u.wins, u.losses, u.league,
		)
	}
}

func seedFollows() {
	fm := map[int][]int{
		1: {2, 3, 4}, 2: {1, 5}, 3: {1, 2}, 4: {1, 2, 7},
		5: {2, 4, 6}, 6: {1, 5, 7}, 7: {2, 4, 6, 8},
		8: {1, 7}, 9: {4, 7, 10}, 10: {1, 2, 4, 7},
	}
	for f, list := range fm {
		for _, t := range list {
			db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, f, t)
		}
	}
}

func seedPosts() {
	type sp struct {
		authorID                                    int
		postType, contentURL, thumbnailURL, caption string
		views                                       int
		createdAt                                   string
	}
	data := []sp{
		{2, "video", "https://cdn.pixabay.com/video/2026/02/09/333600_small.mp4", "https://cdn.pixabay.com/video/2026/02/09/333600_small.jpg", "Diamond league gameplay - watch and learn", 5400, "2026-02-20T14:00:00Z"},
		{7, "video", "", "", "Frozen intime New combo showcase!", 8800, "2026-02-20T13:30:00Z"},
		{1, "video", "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", "Finally broke my personal record", 1200, "2026-02-20T12:45:00Z"},
		{10, "image", "", "", "New setup reveal Ready to dominate!", 3200, "2026-02-20T11:15:00Z"},
		{4, "video", "", "", "Shadow techniques vol.3 - the comeback is real ", 4100, "2026-02-20T10:30:00Z"},
		{5, "video", "", "", "Speed run challenge accepted!", 2100, "2026-02-20T09:00:00Z"},
		{6, "video", "", "", "Storm surge combo into triple elimination", 3800, "2026-02-19T22:00:00Z"},
		{3, "image", "https://pixabay.com/get/g1cb3fb5e78308321688cd47550266beec73006700f8fd3f16549b183bd601669b6128582de0d1c92a4800ae0109174150e040eaeba050fdae70980c53ef8761c_1280.jpg", "", "Just started competing - wish me luck! ", 800, "2026-02-19T20:30:00Z"},
		{9, "video", "", "", "Thunder strike compilation ", 2600, "2026-02-19T18:45:00Z"},
		{8, "video", "", "", "Late-night practice session ", 650, "2026-02-19T17:00:00Z"},
		{2, "video", "https://cdn.pixabay.com/video/2026/01/09/326739_small.mp4", "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg", "1v1 challenge against shadowstrike - who wins?", 9200, "2026-02-19T15:30:00Z"},
		{4, "image", "", "", "Platinum badge unlocked!", 5100, "2026-02-19T14:00:00Z"},
		{7, "video", "", "", "Tutorial: Advanced freeze frame technique", 4200, "2026-02-19T12:15:00Z"},
		{1, "video", "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", "Gold league highlights - best plays this week ", 1800, "2026-02-19T10:30:00Z"},
		{10, "video", "", "", "AI - assisted training results are insane", 7500, "2026-02-19T08:00:00Z"},
		{6, "video", "", "", "Road to Platinum - day 45 of the grind ", 2400, "2026-02-18T23:00:00Z"},
		{5, "image", "", "", "New controller just dropped ", 1600, "2026-02-18T21:00:00Z"},
		{9, "video", "", "", "My best clutch moment yet - 1 HP survival!!", 4800, "2026-02-18T19:00:00Z"},
		{3, "video", "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", "Learning from the pros Watch me improve!", 500, "2026-02-18T17:30:00Z"},
		{8, "video", "", "", "First win of the season ", 900, "2026-02-18T16:00:00Z"},
		{2, "video", "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", "How I got to Diamond in 30 days - full guide", 12000, "2026-02-18T14:00:00Z"},
		{7, "image", "", "", "New character skin unlocked - thoughts?", 3100, "2026-02-18T12:00:00Z"},
		{4, "video", "", "", "Top 5 mistakes beginners make (avoid these!)", 5600, "2026-02-18T10:00:00Z"},
		{10, "video", "", "", "Challenge accepted: 24-hour win streak attempt ", 8900, "2026-02-18T08:00:00Z"},
		{6, "video", "", "", "Storm vs Blaze - epic rivalry match recap ", 3600, "2026-02-17T22:00:00Z"},
		{1, "image", "https://pixabay.com/get/g6d2fb7b4dde02b4febf151624eaf6ee7f096bc81be660c3da10483538d9a3386d3a89ce0a6cfb52722f4c7e824fd261614b56eddbf31da7d1d4f96fcab32dc5e_640.jpg", "", "My journey from Bronze to Gold in one month", 2200, "2026-02-17T18:00:00Z"},
		{5, "video", "", "", "Speed kills - fastest elimination compilation", 2000, "2026-02-17T15:00:00Z"},
		{9, "video", "", "", "Team challenge highlights with frostbyte!", 1700, "2026-02-17T12:00:00Z"},
	}
	for _, p := range data {
		t, _ := time.Parse(time.RFC3339, p.createdAt)
		db.Exec(
			`INSERT INTO posts (author_id, type, content_url, thumbnail_url, caption, views, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			p.authorID, p.postType, p.contentURL, p.thumbnailURL, p.caption, p.views, t,
		)
	}
}

func seedComments() {
	type sc struct {
		postID, authorID int
		text, createdAt  string
	}
	data := []sc{
		{1, 1, "Insane gameplay! Teach me your ways ", "2026-02-20T14:30:00Z"},
		{1, 4, "Diamond players are built different", "2026-02-20T14:45:00Z"},
		{1, 7, "GG, let's run duos sometime", "2026-02-20T15:00:00Z"},
		{2, 2, "That combo is nutty", "2026-02-20T13:45:00Z"},
		{2, 9, "How do you do the freeze cancel?", "2026-02-20T14:00:00Z"},
		{3, 2, "Nice one! Keep grinding ", "2026-02-20T13:00:00Z"},
		{3, 6, "Gold league represent!", "2026-02-20T13:15:00Z"},
		{4, 7, "Clean setup! What monitor is that?", "2026-02-20T11:45:00Z"},
		{5, 10, "Shadow gameplay is always entertaining", "2026-02-20T11:00:00Z"},
		{8, 1, "Welcome to the arena! Good luck", "2026-02-19T21:00:00Z"},
		{8, 5, "We all started somewhere, you got this!", "2026-02-19T21:30:00Z"},
		{11, 4, "Rematch? I want that W back ", "2026-02-19T16:00:00Z"},
		{11, 1, "This match was legendary!", "2026-02-19T16:30:00Z"},
		{12, 2, "Welcome to Platinum! See you in Diamond soon", "2026-02-19T14:30:00Z"},
		{14, 3, "Your highlights are so motivating!", "2026-02-19T11:00:00Z"},
		{21, 8, "This guide helped me so much, thanks!", "2026-02-18T15:00:00Z"},
		{26, 3, "Inspiring journey! I'm working on the same goal", "2026-02-17T19:00:00Z"},
	}
	for _, c := range data {
		t, _ := time.Parse(time.RFC3339, c.createdAt)
		db.Exec(
			`INSERT INTO comments (post_id, author_id, text, created_at) VALUES ($1,$2,$3,$4)`,
			c.postID, c.authorID, c.text, t,
		)
	}
}

// ------------------------------------------------------------------------------
// Challenge CRUD
// ------------------------------------------------------------------------------

// CreateChallenge inserts a new challenge and optional visibility rows.
func CreateChallenge(payload CreateChallengePayload) (Challenge, error) {
	creatorID, err := strconv.Atoi(payload.CreatorID)
	if err != nil {
		return Challenge{}, fmt.Errorf("invalid creator ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		creatorID, payload.VideoURL, payload.ThumbnailURL, payload.Prefix, payload.Subject, payload.Visibility,
	).Scan(&id, &createdAt)
	if err != nil {
		return Challenge{}, err
	}

	// If friends visibility with specific users, insert visibility rows.
	if payload.Visibility == "friends" && len(payload.VisibleTo) > 0 {
		for _, uidStr := range payload.VisibleTo {
			uid, _ := strconv.Atoi(uidStr)
			if uid > 0 {
				db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, id, uid)
			}
		}
	}

	// Fetch the creator info.
	creator, _ := GetUserByID(payload.CreatorID)

	return Challenge{
		ID:              strconv.Itoa(id),
		CreatorID:       payload.CreatorID,
		CreatorUsername: creator.Username,
		CreatorLeague:   creator.League,
		VideoURL:        payload.VideoURL,
		ThumbnailURL:    payload.ThumbnailURL,
		Prefix:          payload.Prefix,
		Subject:         payload.Subject,
		Visibility:      payload.Visibility,
		Status:          "open",
		CreatedAt:       createdAt.UTC().Format(time.RFC3339), ExpiresAt: createdAt.Add(24 * time.Hour).UTC().Format(time.RFC3339)}, nil
}

// challengeBaseQuery is the common SELECT for challenges.
const challengeBaseQuery = `
SELECT c.id, c.creator_id, u.username, u.league,
	COALESCE(c.video_url, '') AS video_url,
	COALESCE(c.thumbnail_url, '') AS thumbnail_url,
	c.prefix, c.subject, c.visibility, c.status, c.views,
	COALESCE(lc.cnt, 0) AS likes,
	COALESCE(rc.cnt, 0) AS response_count,
	c.created_at
FROM challenges c
JOIN users u ON c.creator_id = u.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_likes GROUP BY challenge_id) lc ON lc.challenge_id = c.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_responses GROUP BY challenge_id) rc ON rc.challenge_id = c.id`

// queryChallenges executes a challenge query and returns Challenge structs.
func queryChallenges(query string, args ...interface{}) []Challenge {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryChallenges error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Challenge
	for rows.Next() {
		var id, creatorID, views, likes, respCount int
		var username, league, videoURL, thumbURL, prefix, subject, visibility, status string
		var createdAt time.Time

		if rows.Scan(&id, &creatorID, &username, &league,
			&videoURL, &thumbURL,
			&prefix, &subject, &visibility, &status, &views,
			&likes, &respCount, &createdAt) == nil {

			result = append(result, Challenge{
				ID:              strconv.Itoa(id),
				CreatorID:       strconv.Itoa(creatorID),
				CreatorUsername: username,
				CreatorLeague:   league,
				VideoURL:        videoURL,
				ThumbnailURL:    thumbURL,
				Prefix:          prefix,
				Subject:         subject,
				Visibility:      visibility,
				Status:          status,
				Views:           views,
				Likes:           likes,
				ResponseCount:   respCount,
				CreatedAt:       createdAt.UTC().Format(time.RFC3339),
				ExpiresAt:       createdAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// GetArenaChallenges returns all non-expired open arena challenges (within 24h).
func GetArenaChallenges() []Challenge {
	return queryChallenges(challengeBaseQuery + `
	  WHERE c.visibility = 'arena' 
	  	AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.created_at > NOW() - INTERVAL '24 hours'))
	  ORDER BY c.created_at DESC`)
}

// GetFriendsChallenges returns challenges visible to a specific user (friends-only).
// This includes: challenges by people the user follows with visibility=friends,
// where the user is either in the visible_to list OR the list is empty (all friends).
func GetFriendsChallenges(userID string) []Challenge {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	query := challengeBaseQuery + `
	WHERE c.visibility = 'friends' 
	  AND c.creator_id IN (SELECT following_id FROM follows WHERE follower_id =$1)
	  AND (
		NOT EXISTS (SELECT 1 FROM challenge_visible_to WHERE challenge_id = c.id)
		OR c.id IN (SELECT challenge_id FROM challenge_visible_to WHERE user_id = $1)
	  )
	  AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.created_at > NOW() - INTERVAL '24 hours'))
	ORDER BY c.created_at DESC`
	return queryChallenges(query, uid)
}

// GetChallengeByIDreturns a single challenge.
func GetChallengeByID(idStr string) (Challenge, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Challenge{}, false
	}
	results := queryChallenges(challengeBaseQuery+` WHERE c.id = $1`, id)
	if len(results) == 0 {
		return Challenge{}, false
	}
	return results[0], true
}

// GetChallengeResponses returns all responses to a challenge.
func GetChallengeResponses(challengeID string) []ChallengeResponse {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cr.id, cr.challenge_id, cr.responder_id, u.username, u.league,
				COALESCE(cr.video_url, '') AS video_url,
				COALESCE(cr.thumbnail_url, '') AS thumbnail_url,
				cr.views,
				COALESCE(lc.cnt, 0) AS likes,
				cr.created_at
		 FROM challenge_responses cr
		 JOIN users u ON cr.responder_id = u.id
		 LEFT JOIN (SELECT response_id, COUNT(*) AS cnt FROM challenge_response_likes GROUP BY response_id) lc ON lc.response_id = cr.id
		 WHERE cr.challenge_id = $1
		 ORDER BY cr.created_at ASC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChallengeResponse
	for rows.Next() {
		var id, chalID, respID, views, likes int
		var username, league, videoURL, thumbURL string
		var createdAt time.Time
		if rows.Scan(&id, &chalID, &respID, &username, &league,
			&videoURL, &thumbURL, &views, &likes, &createdAt) == nil {
			result = append(result, ChallengeResponse{
				ID:                strconv.Itoa(id),
				ChallengeID:       strconv.Itoa(chalID),
				ResponderID:       strconv.Itoa(respID),
				ResponderUsername: username,
				ResponderLeague:   league,
				VideoURL:          videoURL,
				ThumbnailURL:      thumbURL,
				Views:             views,
				Likes:             likes,
				CreatedAt:         createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// AcceptChallenge inserts a response and updates challenge status.
func AcceptChallenge(payload AcceptChallengePayload) (ChallengeResponse, error) {
	cid, err1 := strconv.Atoi(payload.ChallengeID)
	rid, err2 := strconv.Atoi(payload.ResponderID)
	if err1 != nil || err2 != nil {
		return ChallengeResponse{}, fmt.Errorf("'invalid IDs")
	}

	var id int
	var createdAt time.Time
	err := db.QueryRow(
		`INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url)
		 VALUES ($1,$2,$3,$4) RETURNING id, created_at`,
		cid, rid, payload.VideoURL, payload.ThumbnailURL,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeResponse{}, err
	}

	// Update challenge status to "active".
	db.Exec(`UPDATE challenges SET status = 'active' WHERE id = $1 AND status = 'open'`, cid)

	responder, _ := GetUserByID(payload.ResponderID)

	return ChallengeResponse{
		ID:                strconv.Itoa(id),
		ChallengeID:       payload.ChallengeID,
		ResponderID:       payload.ResponderID,
		ResponderUsername: responder.Username,
		ResponderLeague:   responder.League,
		VideoURL:          payload.VideoURL,
		ThumbnailURL:      payload.ThumbnailURL,
		CreatedAt:         createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// ToggleChallengeLike toggles a like on a challenge.
func ToggleChallengeLike(challengeID, userID string) (bool, int) {
	cid, e1 := strconv.Atoi(challengeID)
	uid, e2 := strconv.Atoi(userID)
	if e1 != nil || e2 != nil {
		return false, 0
	}

	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2)`, cid, uid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
	} else {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cid, uid)
	}

	var count int
	db.QueryRow(`SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=$1`, cid).Scan(&count)
	return !exists, count
}

// IncrementChallengeViews bumps the view count for a challenge.
func IncrementChallengeViews(challengeID string) {
	cid, err := strconv.Atoi(challengeID)
	if err == nil {
		db.Exec(`UPDATE challenges SET views = views + 1 WHERE id = $1`, cid)
	}
}

// GetHomeFeed returns a mixed feed: 3 challenges then 1 post, repeating.
// Includes all active/open challenges and all posts, newest first.
func GetHomeFeed() []HomeFeedItem {
	// Fetch all displayable challenges (not expired if open).
	challenges := queryChallenges(challengeBaseQuery + `
	  Where (c.status IN ('active','completed') OR (c.status = 'open' AND c.created_at > NOW() - INTERVAL '24 hours'))
      ORDER BY c.created_at DESC`)

	// Fetch all posts newest first.
	posts, _ := GetPostsPaginated(1, 100)

	var result []HomeFeedItem
	ci, pi := 0, 0
	for ci < len(challenges) || pi < len(posts) {
		// Add up to 3 challenges.
		for j := 0; j < 3 && ci < len(challenges); j++ {
			c := challenges[ci]
			result = append(result, HomeFeedItem{Type: "challenge", Challenge: &c})
			ci++
		}
		// Add 1 post.
		if pi < len(posts) {
			p := posts[pi]
			result = append(result, HomeFeedItem{Type: "post", Post: &p})
			pi++
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Watch Events CRUD
// ---------------------------------------------------------------------------

// RecordWatchEvent inserts a watch event for analytics.
func RecordWatchEvent(payload WatchEventPayload) error {
	uid, err := strconv.Atoi(payload.UserID)
	if err != nil {
		return fmt.Errorf("invalid user ID")
	}
	cid, err := strconv.Atoi(payload.ContentID)
	if err != nil {
		return fmt.Errorf("invalid content ID")
	}

	_, err = db.Exec(
		`INSERT INTO watch_events (user_id, content_id, content_type, watch_time, completed)
		 VALUES ($1,$2,$3,$4,$5)`,
		uid, cid, payload.ContentType, payload.WatchTime, payload.Completed,
	)
	return err
}

// ---------------------------------------------------------------------------
// Challenge Votes CRUD
// ---------------------------------------------------------------------------

// CastVote records a user's vote on a challenge response. One vote per user per challenge.
func CastVote(payload ChallengeVotePayload) (bool, error) {
	cid, err := strconv.Atoi(payload.ChallengeID)
	if err != nil {
		return false, fmt.Errorf("invalid challenge ID")
	}
	rid, err := strconv.Atoi(payload.ResponseID)
	if err != nil {
		return false, fmt.Errorf("invalid response ID")
	}
	vid, err := strconv.Atoi(payload.VoterID)
	if err != nil {
		return false, fmt.Errorf("invalid voter ID")
	}

	// Upsert: if user already voted, update their vote
	_, err = db.Exec(
		`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (challenge_id, voter_id)
		 DO UPDATE SET response_id = $2`,
		cid, rid, vid,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetVoteSummary returns vote counts per response for a challenge.
func GetVoteSummary(challengeID string) []VoteSummary {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cv.response_id, u.username, COUNT(*) AS votes
		 FROM challenge_votes cv
		 JOIN challenge_responses cr ON cv.response_id = cr.id
		 JOIN users u ON cr.responder_id = u.id
		 WHERE cv.challenge_id = $1
		 GROUP BY cv.response_id, u.username
		 ORDER BY votes DESC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []VoteSummary
	for rows.Next() {
		var respID, votes int
		var username string
		if rows.Scan(&respID, &username, &votes) == nil {
			result = append(result, VoteSummary{
				ResponseID: strconv.Itoa(respID),
				Username:   username,
				Votes:      votes,
			})
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Reports CRUD
// ---------------------------------------------------------------------------

// CreateReport inserts a new content/user report.
func CreateReport(payload ReportPayload) (Report, error) {
	reporterID, err := strconv.Atoi(payload.ReporterID)
	if err != nil {
		return Report{}, fmt.Errorf("invalid reporter ID")
	}
	targetID, err := strconv.Atoi(payload.TargetID)
	if err != nil {
		return Report{}, fmt.Errorf("invalid target ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO reports (reporter_id, target_id, target_type, reason, description)
		 VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
		reporterID, targetID, payload.TargetType, payload.Reason, payload.Description,
	).Scan(&id, &createdAt)
	if err != nil {
		return Report{}, err
	}

	return Report{
		ID:          strconv.Itoa(id),
		ReporterID:  payload.ReporterID,
		TargetID:    payload.TargetID,
		TargetType:  payload.TargetType,
		Reason:      payload.Reason,
		Description: payload.Description,
		Status:      "pending",
		CreatedAt:   createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// --------------------------------------------------------------------------------
// Seed challenges (sample data)
// --------------------------------------------------------------------------------

func seedChallenges() {
	type sc struct {
		creatorID          int
		videoURL, thumbURL string
		prefix, subject    string
		visibility, status string
		views              int
		createdAt          string
	}

	// Public sample video URLs (all verified accessible)
	v1 := "https://cdn.pixabay.com/video/2026/01/28/331030_medium.mp4"
	v2 := "https://cdn.pixabay.com/video/2021/06/06/76681-559745365_medium.mp4"
	v3 := "https://cdn.pixabay.com/video/2026/02/17/335040_medium.mp4"
	v4 := "https://cdn.pixabay.com/video/2026/02/10/333819_medium.mp4"
	v5 := "https://cdn.pixabay.com/video/2026/01/05/326081_medium.mp4"
	v6 := "https://flutter.github.io/assets-for-api-docs/assets/videos/butterfly.mp4"
	v7 := "https://flutter.github.io/assets-for-api-docs/assets/videos/bee.mp4"
	v8 := "https://media.w3.org/2010/05/sintel/trailer.mp4"
	v9 := "https://media.w3.org/2010/05/bunny/trailer.mp4"
	v10 := "https://media.w3.org/2010/05/bunny/movie.mp4"
	v11 := "https://cdn.plyr.io/static/demo/View_From_A_Blue_Moon_Trailer-720p.mp4"
	v12 := "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_1MB.mp4"
	v13 := "https://test-videos.co.uk/vids/jellyfish/mp4/h264/720/Jellyfish_720_10s_1MB.mp4"
	v14 := "https://test-videos.co.uk/vids/sintel/mp4/h264/720/Sintel_720_10s_1MB.mp4"
	v15 := "https://www.w3schools.com/html/mov_bbb.mp4"

	data := []sc{
		// === SHORTS (open, no one accepted — single video) ===
		// -- player1 (id 1) --
		{1, v3, "", "Who can beat", "This Record", "arena", "open", 950, "2026-04-02T06:00:00Z"},
		{1, v10, "", "Who is faster at", "Speed Run", "arena", "open", 1200, "2026-04-02T07:00:00Z"},
		{1, v7, "", "Who has the best", "Morning Routine", "arena", "open", 2400, "2026-04-02T08:00:00Z"},

		// -- player2 (id 2) --
		{2, v1, "", "Who is better at", "Dance Moves", "arena", "open", 3200, "2026-04-02T09:00:00Z"},
		{2, v12, "", "Who can do a better", "Freestyle Rap", "arena", "open", 2800, "2026-04-02T10:00:00Z"},
		{2, v9, "", "Who can pull off", "This Look", "arena", "open", 1700, "2026-04-02T11:00:00Z"},

		// -- player3 (id 3) --
		{3, v6, "", "Who has the best", "Cooking Skills", "arena", "open", 4200, "2026-04-02T12:00:00Z"},
		{3, v11, "", "Who plays better", "Guitar Solo", "arena", "open", 1300, "2026-04-02T13:00:00Z"},

		// -- shadowstrike (id 4) --
		{4, v11, "", "Which is the best", "Music Cover", "arena", "open", 5600, "2026-04-02T06:30:00Z"},
		{4, v4, "", "Who has better", "Reflexes", "arena", "open", 3900, "2026-04-02T07:30:00Z"},
		{4, v15, "", "Who can land", "This Combo", "arena", "open", 2100, "2026-04-02T08:30:00Z"},

		// -- blazerunner (id 5) --
		{5, v5, "", "Who can do better", "Trick Shot", "arena", "open", 1500, "2026-04-02T09:30:00Z"},
		{5, v8, "", "Who is funnier", "Stand Up Bit", "arena", "open", 4100, "2026-04-02T10:30:00Z"},
		{5, v14, "", "Who runs faster", "Sprint Challenge", "arena", "open", 2600, "2026-04-02T11:30:00Z"},

		// -- stormchaser (id 6) --
		{6, v9, "", "Who has better", "Fashion Style", "arena", "open", 3400, "2026-04-02T12:30:00Z"},
		{6, v2, "", "Who has the cooler", "Room Tour", "arena", "open", 1800, "2026-04-02T13:30:00Z"},

		// -- frostbyte (id 7) --
		{7, v2, "", "Which is the best", "Gaming Setup", "arena", "open", 1800, "2026-04-02T06:15:00Z"},
		{7, v13, "", "Who has the better", "Sports Move", "arena", "open", 1900, "2026-04-02T07:15:00Z"},
		{7, v6, "", "Who cooks better", "Ramen Bowl", "arena", "open", 3500, "2026-04-02T08:15:00Z"},

		// -- nightowl (id 8) --
		{8, v7, "", "Which is more", "Creative Art", "arena", "open", 890, "2026-04-02T09:15:00Z"},
		{8, v12, "", "Who draws better", "Anime Character", "arena", "open", 1100, "2026-04-02T10:15:00Z"},
		{8, v3, "", "Who has better", "Night Photography", "arena", "open", 750, "2026-04-02T11:15:00Z"},

		// -- thunderbolt (id 9) --
		{9, v8, "", "Who can nail", "This Comedy Bit", "arena", "open", 6700, "2026-04-02T12:15:00Z"},
		{9, v14, "", "Who throws better", "Javelin Throw", "arena", "open", 2200, "2026-04-02T13:15:00Z"},
		{9, v1, "", "Who has the better", "Dance Routine", "arena", "open", 3800, "2026-04-02T06:45:00Z"},

		// -- cyberking (id 10) --
		{10, v4, "", "Who is the real", "Champion", "arena", "open", 2100, "2026-04-02T07:45:00Z"},
		{10, v10, "", "Who builds faster", "PC Build Race", "arena", "open", 5200, "2026-04-02T08:45:00Z"},
		{10, v15, "", "Who codes faster", "Bug Fix Race", "arena", "open", 4400, "2026-04-02T09:45:00Z"},

		// === BATTLES (accepted — both challenger and opponent have videos) ===
		{4, v14, "", "Who has better", "Strategy", "arena", "active", 4500, "2026-03-27T16:00:00Z"},      // challenge 31 — battle
		{6, v15, "", "Who dances better", "Salsa", "arena", "active", 7200, "2026-03-28T10:00:00Z"},      // challenge 32 — battle
		{10, v1, "", "Who sings better", "Pop Song", "arena", "active", 3100, "2026-03-26T19:00:00Z"},    // challenge 33 — battle
		{1, v5, "", "Who flips better", "Pancake Flip", "arena", "active", 2900, "2026-03-30T08:00:00Z"}, // challenge 34 — battle
		{9, v11, "", "Who plays better", "Drum Solo", "arena", "active", 5100, "2026-03-31T14:00:00Z"},   // challenge 35 — battle
		{5, v3, "", "Who skates better", "Kickflip", "arena", "active", 6300, "2026-03-29T17:00:00Z"},    // challenge 36 — battle
		{7, v9, "", "Who styles better", "Outfit Check", "arena", "active", 4700, "2026-03-30T19:00:00Z"},// challenge 37 — battle

		// === FRIENDS-ONLY (some open, some battles) ===
		{2, v5, "", "Who is better", "Sniper", "friends", "open", 400, "2026-03-27T18:00:00Z"},
		{8, v8, "", "Who cooks better", "Pasta", "friends", "active", 560, "2026-03-28T07:00:00Z"},        // challenge 39 — battle
	}

	for _, c := range data {
		t, _ := time.Parse(time.RFC3339, c.createdAt)
		db.Exec(
			`INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility, status, views, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			c.creatorID, c.videoURL, c.thumbURL, c.prefix, c.subject, c.visibility, c.status, c.views, t,
		)
	}

	// Seed challenge likes — spread across all challenges.
	// {challengeID, userID}
	challengeLikes := [][2]int{
		// shorts likes
		{1, 2}, {1, 4}, {1, 6}, {1, 8}, {1, 10},
		{2, 3}, {2, 5}, {2, 7}, {2, 9},
		{3, 1}, {3, 4}, {3, 6},
		{4, 1}, {4, 3}, {4, 5}, {4, 7}, {4, 9}, {4, 10},
		{5, 2}, {5, 6}, {5, 8},
		{6, 1}, {6, 4}, {6, 7}, {6, 9},
		{7, 2}, {7, 5}, {7, 8}, {7, 10},
		{8, 1}, {8, 3}, {8, 6},
		{9, 2}, {9, 4}, {9, 7}, {9, 8}, {9, 10},
		{10, 1}, {10, 3}, {10, 5}, {10, 9},
		{11, 2}, {11, 6}, {11, 7},
		{12, 1}, {12, 3}, {12, 8}, {12, 10},
		{13, 4}, {13, 5}, {13, 9},
		{14, 1}, {14, 2}, {14, 7},
		{15, 3}, {15, 6}, {15, 10},
		{16, 1}, {16, 4}, {16, 8},
		{17, 2}, {17, 5}, {17, 9}, {17, 10},
		{18, 1}, {18, 3}, {18, 6}, {18, 7},
		{19, 2}, {19, 4}, {19, 8}, {19, 9}, {19, 10},
		{20, 1}, {20, 5}, {20, 7},
		{21, 3}, {21, 6}, {21, 9},
		{22, 2}, {22, 4}, {22, 10},
		{23, 1}, {23, 5}, {23, 8},
		{24, 3}, {24, 7}, {24, 9}, {24, 10},
		{25, 1}, {25, 2}, {25, 4}, {25, 6},
		{26, 3}, {26, 5}, {26, 8},
		{27, 1}, {27, 7}, {27, 9},
		{28, 2}, {28, 4}, {28, 6}, {28, 10},
		// battle likes (battles are IDs 29-35, friends 36-37)
		{29, 1}, {29, 3}, {29, 5}, {29, 7}, {29, 9}, {29, 10},
		{30, 2}, {30, 4}, {30, 6}, {30, 8}, {30, 1}, {30, 3}, {30, 9},
		{31, 1}, {31, 5}, {31, 7},
		{32, 2}, {32, 4}, {32, 6}, {32, 8}, {32, 10},
		{33, 1}, {33, 3}, {33, 5}, {33, 7},
		{34, 2}, {34, 4}, {34, 6}, {34, 8}, {34, 9}, {34, 10},
		{35, 1}, {35, 3}, {35, 5}, {35, 8},
	}
	for _, cl := range challengeLikes {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cl[0], cl[1])
	}

	// Seed responses for battle challenges.
	// 28 shorts (IDs 1-28), 7 battles (IDs 29-35), 2 friends (IDs 36-37).
	type sr struct {
		challengeID, responderID int
		videoURL, thumbURL       string
		views                    int
		createdAt                string
	}
	responses := []sr{
		{29, 2, v2, "", 3100, "2026-03-27T17:30:00Z"},    // player2 responds to shadowstrike
		{30, 9, v7, "", 5800, "2026-03-28T11:30:00Z"},    // thunderbolt responds to stormchaser
		{31, 3, v10, "", 2200, "2026-03-26T21:00:00Z"},   // player3 responds to cyberking
		{32, 7, v12, "", 2700, "2026-03-30T10:00:00Z"},   // frostbyte responds to player1
		{33, 4, v6, "", 4200, "2026-03-31T16:00:00Z"},    // shadowstrike responds to thunderbolt
		{34, 10, v15, "", 5100, "2026-03-29T19:00:00Z"},  // cyberking responds to blazerunner
		{35, 2, v4, "", 3900, "2026-03-30T21:00:00Z"},    // player2 responds to frostbyte
		{37, 1, v3, "", 340, "2026-03-28T08:00:00Z"},     // player1 responds to nightowl (friends battle)
	}
	for _, r := range responses {
		rt, _ := time.Parse(time.RFC3339, r.createdAt)
		db.Exec(
			`INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url, views, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			r.challengeID, r.responderID, r.videoURL, r.thumbURL, r.views, rt,
		)
	}

	// Seed votes on battle challenges.
	type sv struct {
		challengeID, responseID, voterID int
	}
	votes := []sv{
		{29, 1, 1}, {29, 1, 5}, {29, 1, 8}, {29, 1, 3}, {29, 1, 10},
		{30, 2, 1}, {30, 2, 4}, {30, 2, 7}, {30, 2, 10}, {30, 2, 2}, {30, 2, 5},
		{31, 3, 2}, {31, 3, 6},
		{32, 4, 3}, {32, 4, 5}, {32, 4, 9},
		{33, 5, 1}, {33, 5, 2}, {33, 5, 6}, {33, 5, 8}, {33, 5, 10},
		{34, 6, 1}, {34, 6, 3}, {34, 6, 7}, {34, 6, 9},
		{35, 7, 1}, {35, 7, 4}, {35, 7, 6}, {35, 7, 10},
	}
	for _, v := range votes {
		db.Exec(
			`INSERT INTO challenge_votes (challenge_id, response_id, voter_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
			v.challengeID, v.responseID, v.voterID,
		)
	}
}

// --------------------------------------------------------------------------
// Chat
// --------------------------------------------------------------------------

// SendChatMessage inserts a message and returns its ID.
func SendChatMessage(senderID, receiverID int, message string, replyToID *int) (int, error) {
	var id int
	err := db.QueryRow(
		`INSERT INTO chat_messages (sender_id, receiver_id, message, reply_to_id)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		senderID, receiverID, message, replyToID,
	).Scan(&id)
	return id, err
}

// GetChatMessages returns messages between two users, newest first.
func GetChatMessages(userA, userB, limit, offset int) []ChatMessage {
	rows, err := db.Query(
		`SELECT m.id, m.sender_id, s.username, m.receiver_id, r.username,
				m.message, m.is_read,
				COALESCE(m.status, 'sent') AS status,
				COALESCE(m.is_edited, FALSE) AS is_edited,
				COALESCE(m.is_deleted, FALSE) AS is_deleted,
				m.reply_to_id,
				(SELECT m2.message FROM chat_messages m2 WHERE m2.id = m.reply_to_id) AS reply_to_text,
				m.created_at
		 FROM chat_messages m
		 JOIN users s ON m.sender_id = s.id
		 JOIN users r ON m.receiver_id = r.id
		 WHERE (m.sender_id = $1 AND m.receiver_id = $2)
		    OR (m.sender_id = $2 AND m.receiver_id = $1)
		 ORDER BY m.created_at DESC
		 LIMIT $3 OFFSET $4`,
		userA, userB, limit, offset,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChatMessage
	for rows.Next() {
		var id, sID, rID int
		var sName, rName, msg, status string
		var isRead, isEdited, isDeleted bool
		var replyToID *int
		var replyToText *string
		var createdAt time.Time
		if rows.Scan(&id, &sID, &sName, &rID, &rName, &msg, &isRead,
			&status, &isEdited, &isDeleted, &replyToID, &replyToText, &createdAt) == nil {
			cm := ChatMessage{
				ID:              strconv.Itoa(id),
				SenderID:        strconv.Itoa(sID),
				SenderUsername:   sName,
				ReceiverID:      strconv.Itoa(rID),
				ReceiverUsername: rName,
				Message:         msg,
				IsRead:          isRead,
				Status:          status,
				IsEdited:        isEdited,
				IsDeleted:       isDeleted,
				CreatedAt:       createdAt.UTC().Format(time.RFC3339),
			}
			if replyToID != nil {
				cm.ReplyToID = strconv.Itoa(*replyToID)
			}
			if replyToText != nil {
				cm.ReplyToText = *replyToText
			}
			result = append(result, cm)
		}
	}
	return result
}

// MarkMessagesRead marks all messages from sender to receiver as read.
func MarkMessagesRead(senderID, receiverID int) {
	db.Exec(
		`UPDATE chat_messages SET is_read = TRUE
		 WHERE sender_id = $1 AND receiver_id = $2 AND is_read = FALSE`,
		senderID, receiverID,
	)
}

// GetConversations returns the list of users the given user has chatted with.
func GetConversations(userID int) []Conversation {
	rows, err := db.Query(
		`SELECT
			CASE WHEN m.sender_id = $1 THEN m.receiver_id ELSE m.sender_id END AS other_id,
			u.username, u.league,
			(SELECT CASE WHEN COALESCE(m2.is_deleted, FALSE) THEN 'This message was deleted' ELSE m2.message END
			 FROM chat_messages m2
			 WHERE (m2.sender_id = $1 AND m2.receiver_id = CASE WHEN m.sender_id = $1 THEN m.receiver_id ELSE m.sender_id END)
			    OR (m2.receiver_id = $1 AND m2.sender_id = CASE WHEN m.sender_id = $1 THEN m.receiver_id ELSE m.sender_id END)
			 ORDER BY m2.created_at DESC LIMIT 1) AS last_msg,
			MAX(m.created_at) AS last_time,
			COUNT(*) FILTER (WHERE m.receiver_id = $1 AND m.is_read = FALSE AND m.sender_id != $1) AS unread
		 FROM chat_messages m
		 JOIN users u ON u.id = CASE WHEN m.sender_id = $1 THEN m.receiver_id ELSE m.sender_id END
		 WHERE m.sender_id = $1 OR m.receiver_id = $1
		 GROUP BY other_id, u.username, u.league
		 ORDER BY last_time DESC`,
		userID,
	)
	if err != nil {
		log.Printf("GetConversations error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Conversation
	for rows.Next() {
		var otherID, unread int
		var username, league string
		var lastMsg *string
		var lastTime time.Time
		if err := rows.Scan(&otherID, &username, &league, &lastMsg, &lastTime, &unread); err != nil {
			log.Printf("GetConversations scan error: %v", err)
			continue
		}
		lm := ""
		if lastMsg != nil {
			lm = *lastMsg
		}
		result = append(result, Conversation{
			UserID:      strconv.Itoa(otherID),
			Username:    username,
			League:      league,
			LastMessage:  lm,
			LastTime:     lastTime.UTC().Format(time.RFC3339),
			UnreadCount: unread,
		})
	}
	return result
}

// ---------------------------------------------------------------------------
// Challenge Comments CRUD
// ---------------------------------------------------------------------------

// AddChallengeComment inserts a new comment on a challenge.
func AddChallengeComment(challengeID, authorID, authorUsername, text string) (ChallengeComment, error) {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return ChallengeComment{}, fmt.Errorf("invalid challenge ID")
	}
	aid, err := strconv.Atoi(authorID)
	if err != nil {
		return ChallengeComment{}, fmt.Errorf("invalid author ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO challenge_comments (challenge_id, author_id, text)
		 VALUES ($1,$2,$3) RETURNING id, created_at`,
		cid, aid, text,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeComment{}, err
	}

	return ChallengeComment{
		ID:             strconv.Itoa(id),
		ChallengeID:    challengeID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// GetChallengeComments fetches all comments for a challenge, oldest first.
func GetChallengeComments(challengeID string) []ChallengeComment {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cc.id, cc.challenge_id, cc.author_id, u.username, cc.text, cc.created_at
		 FROM challenge_comments cc
		 JOIN users u ON cc.author_id = u.id
		 WHERE cc.challenge_id = $1
		 ORDER BY cc.created_at ASC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChallengeComment
	for rows.Next() {
		var commentID, challengeIDInt, authorID int
		var username, text string
		var createdAt time.Time
		if rows.Scan(&commentID, &challengeIDInt, &authorID, &username, &text, &createdAt) == nil {
			result = append(result, ChallengeComment{
				ID:             strconv.Itoa(commentID),
				ChallengeID:    strconv.Itoa(challengeIDInt),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				Text:           text,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Saved Challenges
// ---------------------------------------------------------------------------

// ToggleSaveChallenge saves or unsaves a challenge for a user. Returns (saved, error).
func ToggleSaveChallenge(userID, challengeID string) (bool, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return false, fmt.Errorf("invalid user ID")
	}
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return false, fmt.Errorf("invalid challenge ID")
	}

	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM saved_challenges WHERE user_id=$1 AND challenge_id=$2)`, uid, cid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM saved_challenges WHERE user_id=$1 AND challenge_id=$2`, uid, cid)
		return false, nil
	}
	_, err = db.Exec(`INSERT INTO saved_challenges (user_id, challenge_id) VALUES ($1,$2)`, uid, cid)
	return true, err
}

// GetSavedChallenges returns all saved challenges for a user.
func GetSavedChallenges(userID string) []Challenge {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT c.id, c.creator_id, u.username, u.league, c.video_url, c.thumbnail_url,
		        c.prefix, c.subject, c.visibility, c.status, c.views, c.created_at,
		        (SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=c.id) AS likes,
		        (SELECT COUNT(*) FROM challenge_responses WHERE challenge_id=c.id) AS response_count
		 FROM saved_challenges sc
		 JOIN challenges c ON sc.challenge_id = c.id
		 JOIN users u ON c.creator_id = u.id
		 WHERE sc.user_id = $1
		 ORDER BY sc.created_at DESC`, uid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Challenge
	for rows.Next() {
		var c Challenge
		var cid, creatorID, views, likes, respCount int
		var createdAt time.Time
		if rows.Scan(&cid, &creatorID, &c.CreatorUsername, &c.CreatorLeague,
			&c.VideoURL, &c.ThumbnailURL, &c.Prefix, &c.Subject,
			&c.Visibility, &c.Status, &views, &createdAt, &likes, &respCount) == nil {
			c.ID = strconv.Itoa(cid)
			c.CreatorID = strconv.Itoa(creatorID)
			c.Views = views
			c.Likes = likes
			c.ResponseCount = respCount
			c.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			result = append(result, c)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Chat Message Operations
// ---------------------------------------------------------------------------

// EditChatMessage updates the text of a message (only by sender, within 15 minutes).
func EditChatMessage(msgID int, senderID int, newText string) error {
	result, err := db.Exec(
		`UPDATE chat_messages SET message=$1, is_edited=TRUE, edited_at=NOW()
		 WHERE id=$2 AND sender_id=$3 AND is_deleted=FALSE
		   AND created_at > NOW() - INTERVAL '15 minutes'`,
		newText, msgID, senderID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found, not yours, or edit window expired (15 min)")
	}
	return nil
}

// DeleteChatMessage soft-deletes a message (unsend for everyone).
func DeleteChatMessage(msgID int, senderID int) error {
	result, err := db.Exec(
		`UPDATE chat_messages SET is_deleted=TRUE, message='This message was deleted'
		 WHERE id=$1 AND sender_id=$2`,
		msgID, senderID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found or not yours")
	}
	return nil
}

// UpdateUserLastSeen updates the user's last_seen timestamp.
func UpdateUserLastSeen(username string) {
	db.Exec(`UPDATE users SET last_seen=NOW() WHERE username=$1`, username)
}

// GetUserLastSeen returns the last_seen timestamp for a user.
func GetUserLastSeen(username string) string {
	var lastSeen time.Time
	err := db.QueryRow(`SELECT COALESCE(last_seen, NOW()) FROM users WHERE username=$1`, username).Scan(&lastSeen)
	if err != nil {
		return ""
	}
	return lastSeen.UTC().Format(time.RFC3339)
}
