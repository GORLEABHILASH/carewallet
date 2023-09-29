const express = require('express');
const Snowflake = require('snowflake-sdk');

const app = express();
const port = 3000; // You can change the port as needed

// Snowflake connection parameters
const snowflakeConfig = {
  account: 'QCB25435',
  username: 'EVANEXSMITH',
  password: 'evanexsmith',
  warehouse: 'COMPUTE_WH',
  database: 'CAREWALLTET_CDB',
  schema: 'PUBLIC', // Change to your schema - its public
};

// Route to fetch data from Snowflake
// Route to fetch data from Snowflake
// Route to fetch data from Snowflake
app.get('/fetchdata', async (req, res) => {
  let connection;

  try {
    // Create a new Snowflake connection
    connection = Snowflake.createConnection(snowflakeConfig);

    // Connect to Snowflake
    await connection.connect();

    // Check if the connection is up
    if (!connection.isUp()) {
      // Handle reconnection or error here
      throw new Error('Snowflake connection is not up.');
    }

    // Execute the query
    const queryResult = await connection.execute({
      sqlText: 'SELECT * FROM APPXCDB_Dump',
    });

    const rows = [];

    // Stream and process rows
    queryResult.streamRows()
      .on('data', (row) => {
        // Process each row here
        rows.push(row);
      })
      .on('end', () => {
        // All rows have been fetched, you can send the response here
        res.json(rows);
      });

    // Handle any connection-related errors
    connection.on('error', (err) => {
      console.error('Snowflake Connection Error:', err);
      res.status(500).json({ error: 'An error occurred while fetching data' });
    });
  } catch (err) {
    console.error('Snowflake Query Error:', err);
    res.status(500).json({ error: 'An error occurred while fetching data' });
  } finally {
    // Ensure the connection is destroyed
    if (connection) {
      connection.destroy();
    }
  }
});






// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
