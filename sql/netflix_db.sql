-- USER AND PROFILE MANAGEMENT

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE profiles (
    profile_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    age INT CHECK (age >= 0),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- SUBSCRIPTION AND PAYMENT

CREATE TABLE subscription_plans (
    plan_id SERIAL PRIMARY KEY,
    plan_name VARCHAR(50) NOT NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    max_devices INT NOT NULL CHECK (max_devices > 0)
);

CREATE TABLE subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    plan_id INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(10) CHECK (status IN ('active', 'expired')),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (plan_id) REFERENCES subscription_plans(plan_id)
);
-- MOVIES, SHOWS AND EPISODES

CREATE TABLE movies_shows (
    content_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content_type VARCHAR(10) CHECK (content_type IN ('movie', 'tv_show')),
    release_year INT CHECK (release_year > 1900),
    language VARCHAR(50),
    duration INT CHECK (duration >= 0), -- Only for movies
    total_seasons INT CHECK (total_seasons >= 0) -- Only for TV shows
);

CREATE TABLE episodes (
    episode_id SERIAL PRIMARY KEY,
    content_id INT NOT NULL,
    season_number INT NOT NULL CHECK (season_number > 0),
    episode_number INT NOT NULL CHECK (episode_number > 0),
    title VARCHAR(255) NOT NULL,
    duration INT NOT NULL CHECK (duration > 0),
    FOREIGN KEY (content_id) REFERENCES movies_shows(content_id) ON DELETE CASCADE
);
-- GENRES AND CATEGORIES

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE content_genres (
    content_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (content_id, genre_id),
    FOREIGN KEY (content_id) REFERENCES movies_shows(content_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);
-- WATCH HISTORY AND RATINGS

CREATE TABLE watch_history (
    history_id SERIAL PRIMARY KEY,
    profile_id INT NOT NULL,
    content_id INT NOT NULL,
    last_watched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    progress_percentage INT CHECK (progress_percentage BETWEEN 0 AND 100),
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES movies_shows(content_id) ON DELETE CASCADE
);

CREATE TABLE ratings (
    rating_id SERIAL PRIMARY KEY,
    profile_id INT NOT NULL,
    content_id INT NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    review TEXT,
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES movies_shows(content_id) ON DELETE CASCADE
);
