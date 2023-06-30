const express = require('express');
const bodyParser = require('body-parser');
const { Pool } = require('pg');

const app = express();
app.use(bodyParser.json());

// PostgreSQL configuration
const pool = new Pool({
  user: 'oqrmahae',
  host: 'tiny.db.elephantsql.com',
  database: 'oqrmahae',
  password: 'mqQyUC01wuaPs3ywaJpU3Qk9UToh4zqw',
  port: 5432, // default PostgreSQL port
});

// Route: GET /api/v1/longest-duration-movies
app.get('/api/v1/longest-duration-movies', (req, res) => {
  const query = `
    SELECT tconst, primaryTitle, runtimeMinutes, genres
    FROM movies
    ORDER BY runtimeMinutes DESC
    LIMIT 10
  `;

  pool.query(query, (err, result) => {
    if (err) {
      console.error('Error executing the query:', err);
      res.status(500).json({ error: 'Internal Server Error' });
      return;
    }

    res.json(result.rows);
  });
});

// Route: POST /api/v1/new-movie
app.post('/api/v1/new-movie', (req, res) => {
  const { tconst, primaryTitle, runtimeMinutes, genres } = req.body;

  const query = `
    INSERT INTO movies (tconst, primaryTitle, runtimeMinutes, genres)
    VALUES ($1, $2, $3, $4)
  `;

  pool.query(query, [tconst, primaryTitle, runtimeMinutes, genres], (err) => {
    if (err) {
      console.error('Error executing the query:', err);
      res.status(500).json({ error: 'Internal Server Error' });
      return;
    }

    res.json({ message: 'success' });
  });
});

// Route: GET /api/v1/top-rated-movies
app.get('/api/v1/top-rated-movies', (req, res) => {
  const query = `
    SELECT movies.tconst, primaryTitle, genres, averageRating
    FROM movies, ratings 
    WHERE movies.tconst = ratings.tconst
    AND averageRating > 6.0
    ORDER BY averageRating
  `;

  pool.query(query, (err, result) => {
    if (err) {
      console.error('Error executing the query:', err);
      res.status(500).json({ error: 'Internal Server Error' });
      return;
    }

    res.json(result.rows);
  });
});

// Route: GET /api/v1/genre-movies-with-subtotals
app.get('/api/v1/genre-movies-with-subtotals', (req, res) => {
  const query = `
    SELECT genres, primaryTitle, numVotes
    FROM movies, ratings where movies.tconst = ratings.tconst
    UNION
    SELECT genres, 'TOTAL', SUM(numVotes)
    FROM movies, ratings where movies.tconst = ratings.tconst
    GROUP BY genres, ROLLUP (genres) ORDER BY genres;
  `;

  pool.query(query, (err, result) => {
    if (err) {
      console.error('Error executing the query:', err);
      res.status(500).json({ error: 'Internal Server Error' });
      return;
    }

    res.json(result.rows);
  });
});

// Route: POST /api/v1/update-runtime-minutes
app.post('/api/v1/update-runtime-minutes', (req, res) => {
  const query = `
    UPDATE movies
    SET runtimeMinutes = CASE
      WHEN genres = 'Documentary' THEN runtimeMinutes + 15
      WHEN genres = 'Animation' THEN runtimeMinutes + 30
      ELSE runtimeMinutes + 45
    END
  `;

  pool.query(query, (err) => {
    if (err) {
      console.error('Error executing the query:', err);
      res.status(500).json({ error: 'Internal Server Error' });
      return;
    }

    res.json({ message: 'success' });
  });
});

// Start the server
app.listen(3000, () => {
  console.log('Server is running on http://localhost:3000');
});
