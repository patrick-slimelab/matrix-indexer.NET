using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Net.Http.Json;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MatrixIndexer;

public class Program
{
    private static readonly HttpClient Http = new();
    private static IMongoCollection<BsonDocument>? _eventsCollection;
    private static string? _syncToken;
    private static string _stateFile = "sync_token.txt";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Matrix Indexer (.NET)");

        // 1. Config
        var homeserver = GetEnv("MATRIX_HOMESERVER");
        var userId = GetEnv("MATRIX_USER_ID");
        var password = GetEnv("MATRIX_PASSWORD");
        var mongoUri = GetEnv("MONGODB_URI", "mongodb://localhost:27017");
        var mongoDbName = GetEnv("MONGODB_DB", "matrix_index");

        // 2. Mongo
        Console.WriteLine($"Connecting to MongoDB: {mongoUri}");
        var mongoClient = new MongoClient(mongoUri);
        var db = mongoClient.GetDatabase(mongoDbName);
        _eventsCollection = db.GetCollection<BsonDocument>("events");

        // Create indexes
        var indexKeys = Builders<BsonDocument>.IndexKeys.Ascending("event_id");
        var indexOptions = new CreateIndexOptions { Unique = true };
        await _eventsCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(indexKeys, indexOptions));
        Console.WriteLine("MongoDB indexes ensured.");

        // 3. Matrix Login
        if (!homeserver.StartsWith("http")) homeserver = "https://" + homeserver;
        homeserver = homeserver.TrimEnd('/');
        
        Console.WriteLine($"Logging into Matrix: {homeserver} as {userId}");
        
        string accessToken = "";
        try
        {
            var loginData = new
            {
                type = "m.login.password",
                user = userId,
                password = password
            };

            var loginResp = await Http.PostAsJsonAsync($"{homeserver}/_matrix/client/r0/login", loginData);
            loginResp.EnsureSuccessStatusCode();
            
            var loginJson = await loginResp.Content.ReadFromJsonAsync<JsonNode>();
            accessToken = loginJson?["access_token"]?.ToString() ?? throw new Exception("No access token");
            Console.WriteLine("Login successful.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Login failed: {ex.Message}");
            return;
        }

        // Load state
        if (File.Exists(_stateFile))
        {
            _syncToken = await File.ReadAllTextAsync(_stateFile);
            Console.WriteLine($"Loaded sync token: {_syncToken}");
        }

        // 4. Sync Loop
        Http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

        Console.WriteLine("Starting sync loop...");
        while (true)
        {
            try
            {
                var url = $"{homeserver}/_matrix/client/r0/sync?timeout=30000";
                if (!string.IsNullOrEmpty(_syncToken)) url += $"&since={_syncToken}";

                var syncResp = await Http.GetAsync(url);
                if (!syncResp.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Sync failed: {syncResp.StatusCode}");
                    await Task.Delay(5000);
                    continue;
                }

                var root = await syncResp.Content.ReadFromJsonAsync<JsonNode>();
                if (root == null) continue;

                // Update token
                var nextBatch = root["next_batch"]?.ToString();
                if (!string.IsNullOrEmpty(nextBatch))
                {
                    _syncToken = nextBatch;
                    await File.WriteAllTextAsync(_stateFile, _syncToken);
                }

                // Process rooms
                var join = root["rooms"]?["join"]?.AsObject();
                if (join != null)
                {
                    foreach (var room in join)
                    {
                        var roomId = room.Key;
                        var events = room.Value?["timeline"]?["events"]?.AsArray();
                        if (events != null)
                        {
                            await ProcessEvents(roomId, events);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in sync loop: {ex.Message}");
                await Task.Delay(5000);
            }
        }
    }

    private static async Task ProcessEvents(string roomId, JsonArray events)
    {
        var docs = new List<BsonDocument>();

        foreach (var ev in events)
        {
            if (ev == null) continue;
            
            var eventId = ev["event_id"]?.ToString();
            if (string.IsNullOrEmpty(eventId)) continue;

            // Convert JsonNode to BsonDocument generically
            // Simple approach: parse JSON string to BsonDocument
            var doc = BsonDocument.Parse(ev.ToJsonString());
            
            // Ensure roomId is set if missing (it's usually in the wrapper, but just in case)
            if (!doc.Contains("room_id")) doc["room_id"] = roomId;
            
            // Timestamp fix (ensure parsing)
            if (doc.Contains("origin_server_ts") && doc["origin_server_ts"].IsInt64)
            {
                // Keep as is
            }

            docs.Add(doc);
        }

        if (docs.Count > 0 && _eventsCollection != null)
        {
            foreach (var doc in docs)
            {
                try
                {
                    var filter = Builders<BsonDocument>.Filter.Eq("event_id", doc["event_id"]);
                    await _eventsCollection.ReplaceOneAsync(filter, doc, new ReplaceOptions { IsUpsert = true });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error saving event: {ex.Message}");
                }
            }
            Console.WriteLine($"Processed {docs.Count} events from {roomId}");
        }
    }

    private static string GetEnv(string key, string? defaultValue = null)
    {
        var val = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrEmpty(val))
        {
            if (defaultValue != null) return defaultValue;
            throw new Exception($"Missing environment variable: {key}");
        }
        return val;
    }
}
