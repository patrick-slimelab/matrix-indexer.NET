using System;
using System.Text.Json.Nodes;
using System.Net.Http.Json;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MatrixIndexer;

public class Program
{
    private static readonly HttpClient Http = new();
    private static IMongoCollection<BsonDocument>? _eventsCollection;
    private static IMongoCollection<BsonDocument>? _backfillCollection;

    private static string? _syncToken;
    private static string _stateFile = "sync_token.txt";
    private static string _indexerId = "default";

    // Backfill behavior
    private static int _backfillPageSize = 200;
    private static int _backfillWorkers = 2;

    // Room discovery behavior
    private static int _joinedRoomsPollSeconds = 300;

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Matrix Indexer (.NET)");

        // 1. Config
        var homeserver = GetEnv("MATRIX_HOMESERVER");
        var userId = GetEnv("MATRIX_USER_ID");
        var password = GetEnv("MATRIX_PASSWORD");
        var mongoUri = GetEnv("MONGODB_URI", "mongodb://localhost:27017");
        var mongoDbName = GetEnv("MONGODB_DB", "matrix_index");

        _indexerId = GetEnv("INDEXER_ID", userId);
        _stateFile = GetEnv("INDEXER_SYNC_TOKEN_PATH", _stateFile);
        _backfillPageSize = int.Parse(GetEnv("INDEXER_BACKFILL_PAGE_SIZE", _backfillPageSize.ToString()));
        _backfillWorkers = int.Parse(GetEnv("INDEXER_BACKFILL_WORKERS", _backfillWorkers.ToString()));
        _joinedRoomsPollSeconds = int.Parse(GetEnv("INDEXER_JOINED_ROOMS_POLL_SECONDS", _joinedRoomsPollSeconds.ToString()));

        // 2. Mongo
        Console.WriteLine($"Connecting to MongoDB: {mongoUri}");
        var mongoClient = new MongoClient(mongoUri);
        var db = mongoClient.GetDatabase(mongoDbName);
        _eventsCollection = db.GetCollection<BsonDocument>("events");
        _backfillCollection = db.GetCollection<BsonDocument>("room_backfill");

        // Ensure indexes
        await EnsureIndexes();
        Console.WriteLine("MongoDB indexes ensured.");

        // 3. Matrix Login
        if (!homeserver.StartsWith("http")) homeserver = "https://" + homeserver;
        homeserver = homeserver.TrimEnd('/');

        Console.WriteLine($"Logging into Matrix: {homeserver} as {userId} (indexer_id={_indexerId})");

        string accessToken;
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

        // 4. Load state
        if (File.Exists(_stateFile))
        {
            _syncToken = (await File.ReadAllTextAsync(_stateFile)).Trim();
            if (!string.IsNullOrEmpty(_syncToken))
                Console.WriteLine($"Loaded sync token: {_syncToken}");
        }

        // 5. Start live sync loop; after first successful sync start background backfill workers
        Http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

        // Aggressively discover joined rooms (so we seed backfill even for quiet rooms)
        try
        {
            await DiscoverJoinedRoomsAndSeedBackfill(homeserver);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Initial joined_rooms discovery failed: {ex.Message}");
        }

        var backfillStarted = false;

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

                // Process rooms first; only persist next_batch after successful processing
                await ProcessSyncRoomsAndSeedBackfill(root);

                // Update token after processing
                var nextBatch = root["next_batch"]?.ToString();
                if (!string.IsNullOrEmpty(nextBatch))
                {
                    _syncToken = nextBatch;
                    await AtomicWriteAsync(_stateFile, _syncToken);
                }

                if (!backfillStarted)
                {
                    backfillStarted = true;
                    Console.WriteLine($"First successful sync complete. Starting backfill workers: {_backfillWorkers} (pageSize={_backfillPageSize})");
                    for (var i = 0; i < _backfillWorkers; i++)
                    {
                        _ = Task.Run(() => BackfillWorkerLoop(homeserver));
                    }

                    Console.WriteLine($"Starting joined_rooms discovery loop (every {_joinedRoomsPollSeconds}s)...");
                    _ = Task.Run(() => JoinedRoomsDiscoveryLoop(homeserver));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in sync loop: {ex.Message}");
                await Task.Delay(5000);
            }
        }
    }

    private static async Task EnsureIndexes()
    {
        if (_eventsCollection == null || _backfillCollection == null) return;

        // Events: unique by event_id (idempotent upsert)
        var evKeys = Builders<BsonDocument>.IndexKeys.Ascending("event_id");
        var evOptions = new CreateIndexOptions { Unique = true };
        await _eventsCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(evKeys, evOptions));

        // Backfill state: unique by (indexer_id, room_id) so multiple accounts can share one DB safely.
        // Also drop legacy unique {room_id:1} index if present (it blocks multi-account use).
        var existingIndexes = await _backfillCollection.Indexes.ListAsync();
        var existingIndexDocs = await existingIndexes.ToListAsync();
        foreach (var idx in existingIndexDocs)
        {
            var name = idx.GetValue("name", "").ToString();
            var unique = idx.GetValue("unique", false).ToBoolean();
            var keyDoc = idx.GetValue("key", new BsonDocument()).AsBsonDocument;
            var isLegacyRoomOnlyUnique = unique
                && keyDoc.ElementCount == 1
                && keyDoc.Contains("room_id");

            if (isLegacyRoomOnlyUnique && !string.IsNullOrEmpty(name))
            {
                Console.WriteLine($"Dropping legacy backfill index: {name}");
                await _backfillCollection.Indexes.DropOneAsync(name);
            }
        }

        var bfKeys = Builders<BsonDocument>.IndexKeys
            .Ascending("indexer_id")
            .Ascending("room_id");
        var bfOptions = new CreateIndexOptions { Unique = true };
        await _backfillCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(bfKeys, bfOptions));

        // Helpful query index for each account's queue scanning
        var bfQueryKeys = Builders<BsonDocument>.IndexKeys
            .Ascending("indexer_id")
            .Ascending("done")
            .Ascending("updated_at");
        await _backfillCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(bfQueryKeys));
    }

    private static async Task ProcessSyncRoomsAndSeedBackfill(JsonNode root)
    {
        var join = root["rooms"]?["join"]?.AsObject();
        if (join == null) return;

        foreach (var room in join)
        {
            var roomId = room.Key;
            var timeline = room.Value?["timeline"];
            var events = timeline?["events"]?.AsArray();
            if (events != null)
            {
                await ProcessEvents(roomId, events);
            }

            // Seed backfill cursor from prev_batch token
            var prevBatch = timeline?["prev_batch"]?.ToString();
            if (!string.IsNullOrEmpty(prevBatch))
            {
                await SeedBackfillRoom(roomId, prevBatch, source: "sync.prev_batch");
            }
        }
    }

    private static async Task SeedBackfillRoom(string roomId, string cursor, string source)
    {
        if (_backfillCollection == null) return;

        var filter = BackfillRoomFilter(roomId);
        var existing = await _backfillCollection.Find(filter).FirstOrDefaultAsync();
        if (existing != null) return;

        var doc = new BsonDocument
        {
            { "indexer_id", _indexerId },
            { "room_id", roomId },
            { "cursor", cursor },
            { "done", false },
            { "claimed", false },
            { "seed_source", source },
            { "created_at", DateTime.UtcNow },
            { "updated_at", DateTime.UtcNow },
            { "error_count", 0 },
        };

        try
        {
            await _backfillCollection.InsertOneAsync(doc);
            Console.WriteLine($"Seeded backfill cursor for room {roomId} (source={source})");
        }
        catch
        {
            // ignore races
        }
    }

    private static async Task JoinedRoomsDiscoveryLoop(string homeserver)
    {
        while (true)
        {
            try
            {
                await DiscoverJoinedRoomsAndSeedBackfill(homeserver);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"joined_rooms discovery error: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(_joinedRoomsPollSeconds));
        }
    }

    private static async Task DiscoverJoinedRoomsAndSeedBackfill(string homeserver)
    {
        if (_backfillCollection == null) return;

        var resp = await Http.GetAsync($"{homeserver}/_matrix/client/r0/joined_rooms");
        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"joined_rooms failed: {(int)resp.StatusCode} {resp.ReasonPhrase}");
            return;
        }

        var json = await resp.Content.ReadFromJsonAsync<JsonNode>();
        var rooms = json?["joined_rooms"]?.AsArray();
        if (rooms == null) return;

        var seeded = 0;
        foreach (var r in rooms)
        {
            var roomId = r?.ToString();
            if (string.IsNullOrEmpty(roomId)) continue;

            // Already seeded for this indexer account?
            var filter = BackfillRoomFilter(roomId);
            var existing = await _backfillCollection.Find(filter).FirstOrDefaultAsync();
            if (existing != null) continue;

            // Get an initial pagination token by asking for 1 event backward from "now".
            var cursor = await GetInitialBackfillCursorFromMessages(homeserver, roomId);
            if (string.IsNullOrEmpty(cursor))
            {
                // No history / no permission; mark as done so workers don't spin forever.
                await SeedBackfillRoom(roomId, "", source: "joined_rooms.messages.no_cursor");
                await MarkBackfillDone(roomId);
                continue;
            }

            await SeedBackfillRoom(roomId, cursor, source: "joined_rooms.messages.end");
            seeded++;
        }

        if (seeded > 0)
            Console.WriteLine($"joined_rooms discovery: seeded {seeded} room(s) for backfill");
    }

    private static async Task<string?> GetInitialBackfillCursorFromMessages(string homeserver, string roomId)
    {
        try
        {
            var url = $"{homeserver}/_matrix/client/r0/rooms/{Uri.EscapeDataString(roomId)}/messages?dir=b&limit=1";
            var resp = await Http.GetAsync(url);
            if (!resp.IsSuccessStatusCode) return null;

            var json = await resp.Content.ReadFromJsonAsync<JsonNode>();
            if (json == null) return null;

            // If we got a chunk, store it too.
            var chunk = json["chunk"]?.AsArray();
            if (chunk != null && chunk.Count > 0)
            {
                await ProcessEvents(roomId, chunk);
            }

            return json["end"]?.ToString();
        }
        catch
        {
            return null;
        }
    }

    private static async Task BackfillWorkerLoop(string homeserver)
    {
        if (_backfillCollection == null) return;

        while (true)
        {
            try
            {
                var room = await ClaimNextBackfillRoom();
                if (room == null)
                {
                    // No rooms pending; sleep a bit then try again.
                    await Task.Delay(TimeSpan.FromMinutes(10));
                    continue;
                }

                var roomId = room["room_id"].AsString;
                var cursor = room.GetValue("cursor", BsonNull.Value).ToString();

                if (string.IsNullOrEmpty(cursor) || cursor == "BsonNull")
                {
                    await MarkBackfillDone(roomId);
                    continue;
                }

                var url = $"{homeserver}/_matrix/client/r0/rooms/{Uri.EscapeDataString(roomId)}/messages?dir=b&limit={_backfillPageSize}&from={Uri.EscapeDataString(cursor)}";
                var resp = await Http.GetAsync(url);

                if ((int)resp.StatusCode == 429)
                {
                    var retry = resp.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(10);
                    await Task.Delay(retry);
                    await UnclaimRoom(roomId);
                    continue;
                }

                if (!resp.IsSuccessStatusCode)
                {
                    await IncrementBackfillError(roomId, $"HTTP {(int)resp.StatusCode}");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                    await UnclaimRoom(roomId);
                    continue;
                }

                var json = await resp.Content.ReadFromJsonAsync<JsonNode>();
                if (json == null)
                {
                    await UnclaimRoom(roomId);
                    continue;
                }

                var chunk = json["chunk"]?.AsArray();
                var end = json["end"]?.ToString();

                if (chunk != null && chunk.Count > 0)
                {
                    await ProcessEvents(roomId, chunk);
                }

                if (string.IsNullOrEmpty(end) || chunk == null || chunk.Count == 0)
                {
                    await MarkBackfillDone(roomId);
                }
                else
                {
                    await UpdateBackfillCursor(roomId, end);
                }

                await Task.Delay(250);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Backfill worker error: {ex.Message}");
                await Task.Delay(5000);
            }
        }
    }

    private static async Task<BsonDocument?> ClaimNextBackfillRoom()
    {
        if (_backfillCollection == null) return null;

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("indexer_id", _indexerId),
            Builders<BsonDocument>.Filter.Eq("done", false),
            Builders<BsonDocument>.Filter.Ne("claimed", true)
        );

        var update = Builders<BsonDocument>.Update
            .Set("claimed", true)
            .Set("updated_at", DateTime.UtcNow);

        var opts = new FindOneAndUpdateOptions<BsonDocument>
        {
            Sort = Builders<BsonDocument>.Sort.Ascending("updated_at"),
            ReturnDocument = ReturnDocument.After
        };

        return await _backfillCollection.FindOneAndUpdateAsync(filter, update, opts);
    }

    private static async Task UnclaimRoom(string roomId)
    {
        if (_backfillCollection == null) return;
        var filter = BackfillRoomFilter(roomId);
        var update = Builders<BsonDocument>.Update
            .Set("claimed", false)
            .Set("updated_at", DateTime.UtcNow);
        await _backfillCollection.UpdateOneAsync(filter, update);
    }

    private static async Task UpdateBackfillCursor(string roomId, string cursor)
    {
        if (_backfillCollection == null) return;
        var filter = BackfillRoomFilter(roomId);
        var update = Builders<BsonDocument>.Update
            .Set("cursor", cursor)
            .Set("claimed", false)
            .Set("updated_at", DateTime.UtcNow);
        await _backfillCollection.UpdateOneAsync(filter, update);
    }

    private static async Task MarkBackfillDone(string roomId)
    {
        if (_backfillCollection == null) return;
        var filter = BackfillRoomFilter(roomId);
        var update = Builders<BsonDocument>.Update
            .Set("done", true)
            .Set("claimed", false)
            .Set("updated_at", DateTime.UtcNow);
        await _backfillCollection.UpdateOneAsync(filter, update);
        Console.WriteLine($"Backfill complete for room {roomId}");
    }

    private static async Task IncrementBackfillError(string roomId, string reason)
    {
        if (_backfillCollection == null) return;
        var filter = BackfillRoomFilter(roomId);
        var update = Builders<BsonDocument>.Update
            .Inc("error_count", 1)
            .Set("last_error", reason)
            .Set("updated_at", DateTime.UtcNow)
            .Set("claimed", false);
        await _backfillCollection.UpdateOneAsync(filter, update);
        Console.WriteLine($"Backfill error for room {roomId}: {reason}");
    }

    private static FilterDefinition<BsonDocument> BackfillRoomFilter(string roomId)
    {
        return Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("indexer_id", _indexerId),
            Builders<BsonDocument>.Filter.Eq("room_id", roomId)
        );
    }

    private static async Task ProcessEvents(string roomId, JsonArray events)
    {
        if (_eventsCollection == null) return;

        var docs = new List<BsonDocument>();
        foreach (var ev in events)
        {
            if (ev == null) continue;

            var eventId = ev["event_id"]?.ToString();
            if (string.IsNullOrEmpty(eventId)) continue;

            var doc = BsonDocument.Parse(ev.ToJsonString());
            if (!doc.Contains("room_id")) doc["room_id"] = roomId;
            docs.Add(doc);
        }

        if (docs.Count == 0) return;

        foreach (var doc in docs)
        {
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq("event_id", doc["event_id"]);
                await _eventsCollection.ReplaceOneAsync(filter, doc, new ReplaceOptions { IsUpsert = true });
            }
            catch
            {
                // Ignore duplicates or transient errors.
            }
        }

        Console.WriteLine($"Processed {docs.Count} events from {roomId}");
    }

    private static async Task AtomicWriteAsync(string path, string content)
    {
        var tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, content);
        File.Move(tmp, path, true);
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
