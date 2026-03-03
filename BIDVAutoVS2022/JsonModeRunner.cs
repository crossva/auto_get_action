using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using ExcelDataReader;

namespace BIDVAutoVS2022
{
    internal static class JsonModeRunner
    {
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNameCaseInsensitive = true
        };

        public static void RunFromConfig()
        {
            string baseDir = AppContext.BaseDirectory;
            string headerScriptPath = ResolvePath(ConfigurationManager.AppSettings["json_header_script_path"] ?? "script_header.json", baseDir);
            string detailScriptPath = ResolvePath(ConfigurationManager.AppSettings["json_detail_script_path"] ?? "script_detail.json", baseDir);
            string excelPath = ResolvePath(ConfigurationManager.AppSettings["json_data_source_path"] ?? "script_data.csv", baseDir);
            string resultFolder = ResolvePath(ConfigurationManager.AppSettings["json_result_folder"] ?? Path.Combine(baseDir, "json_result"), baseDir);
            string fixedResultPath = ResolvePath(ConfigurationManager.AppSettings["json_fixed_result_file"] ?? Path.Combine(resultFolder, "ket_qua.json"), baseDir);

            Directory.CreateDirectory(resultFolder);

            var headerSteps = ReadScript(headerScriptPath);
            var detailSteps = ReadScript(detailScriptPath);
            var inputRows = ReadInputRows(excelPath);

            bool headerDone = headerSteps.Count == 0;
            if (headerSteps.Count > 0)
            {
                ExecuteSteps(headerSteps, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), Path.GetDirectoryName(detailScriptPath) ?? baseDir);
                headerDone = true;
            }

            var newItems = new List<JsonRunItem>();
            int success = 0;
            int fail = 0;

            foreach (var row in inputRows)
            {
                string stt = row.ContainsKey("stt") ? row["stt"] : string.Empty;
                if (string.IsNullOrWhiteSpace(stt))
                {
                    continue;
                }

                string onOff = row.ContainsKey("on_off") ? row["on_off"] : "1";
                if (onOff != "1")
                {
                    continue;
                }
                
                try
                {
                    ExecuteSteps(detailSteps, row, Path.GetDirectoryName(detailScriptPath) ?? baseDir);
                    newItems.Add(new JsonRunItem
                    {
                        Id = stt,
                        Stt = stt,
                        LanChay = 1,
                        Status = "success",
                        Message = $"Đã xử lý case STT={stt} bằng JSON mode ({detailSteps.Count} bước định nghĩa).",
                        MessageBefore = string.Empty
                    });
                    success++;
                }
                catch (Exception ex)
                {
                    newItems.Add(new JsonRunItem
                    {
                        Id = stt,
                        Stt = stt,
                        LanChay = 1,
                        Status = "error",
                        Message = ex.Message,
                        MessageBefore = string.Empty
                    });
                    fail++;
                }
            }

            var result = new JsonRunResult
            {
                ThoiGian = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                Status = fail == 0 ? "done" : "partial",
                SoLuongSucces = success.ToString(CultureInfo.InvariantCulture),
                SoLuongFail = fail.ToString(CultureInfo.InvariantCulture),
                HeaderDaChay = headerDone,
                Data = newItems
            };

            string stampFile = Path.Combine(resultFolder, $"ketqua_{DateTime.Now:yyyy_MM_dd_HH_mm_ss}.json");
            File.WriteAllText(stampFile, JsonSerializer.Serialize(result, JsonOptions));
            File.WriteAllText(fixedResultPath, JsonSerializer.Serialize(result, JsonOptions));

            Logger.LogInfo($"JSON mode hoàn tất. HeaderSteps={headerSteps.Count}, DetailSteps={detailSteps.Count}, Success={success}, Fail={fail}.");
            Logger.LogInfo($"Kết quả mới: {stampFile}");
        }

        private static void ExecuteSteps(List<Dictionary<string, object?>> steps, Dictionary<string, string> rowValues, string scriptDirectory)
        {
            foreach (var step in steps.OrderBy(x => GetIntValue(x, "order_by", 0)))
            {
                if (!GetBoolValue(step, "hieuluc", true))
                {
                    continue;
                }

                string typeBy = GetStringValue(step, "type_by", "").Trim().ToLowerInvariant();
                string stepName = GetStringValue(step, "name", "(no_name)");
                int beginMs = GetIntValue(step, "begin_time_sleep", 0);
                int inMs = GetIntValue(step, "in_time_sleep", 0);
                int endMs = GetIntValue(step, "end_time_sleep", 0);

                SleepMs(beginMs);
                WaitByInTime(inMs);

                if (typeBy == "internal loop")
                {
                    string fileLoop = GetStringValue(step, "filename_script_internal_loop", "");
                    if (!string.IsNullOrWhiteSpace(fileLoop))
                    {
                        string loopPath = Path.IsPathRooted(fileLoop) ? fileLoop : Path.GetFullPath(Path.Combine(scriptDirectory, fileLoop));
                        var loopSteps = ReadScript(loopPath);
                        ExecuteSteps(loopSteps, rowValues, Path.GetDirectoryName(loopPath) ?? scriptDirectory);
                    }
                }
                else
                {
                    string inputValue = ResolveInputValue(GetStringValue(step, "input_value", ""), rowValues);
                    string selector = ResolveInputValue(GetStringValue(step, "s_value", ""), rowValues);
                    Logger.LogInfo($"[JSON STEP] name={stepName}; type_by={typeBy}; s_value={selector}; input_value={inputValue}; begin={beginMs}; in={inMs}; end={endMs}");
                }

                SleepMs(endMs);
            }
        }

        private static void WaitByInTime(int inMs)
        {
            if (inMs <= 0)
            {
                return;
            }

            int waited = 0;
            int chunk = 200;
            while (waited < inMs)
            {
                SleepMs(Math.Min(chunk, inMs - waited));
                waited += chunk;
            }
        }

        private static void SleepMs(int ms)
        {
            if (ms > 0)
            {
                Thread.Sleep(ms);
            }
        }

        private static string ResolveInputValue(string template, Dictionary<string, string> rowValues)
        {
            if (string.IsNullOrWhiteSpace(template))
            {
                return string.Empty;
            }

            string value = template;
            foreach (var kv in rowValues)
            {
                value = value.Replace("{{" + kv.Key + "}}", kv.Value);
                value = value.Replace(kv.Key, kv.Value);
            }
            return value;
        }

        private static string GetStringValue(Dictionary<string, object?> dic, string key, string defaultValue)
        {
            if (!dic.TryGetValue(key, out object? value) || value == null)
            {
                return defaultValue;
            }

            if (value is JsonElement json)
            {
                return json.ValueKind == JsonValueKind.String ? json.GetString() ?? defaultValue : json.ToString();
            }

            return value.ToString() ?? defaultValue;
        }

        private static int GetIntValue(Dictionary<string, object?> dic, string key, int defaultValue)
        {
            if (!dic.TryGetValue(key, out object? value) || value == null)
            {
                return defaultValue;
            }

            if (value is JsonElement json)
            {
                if (json.ValueKind == JsonValueKind.Number && json.TryGetInt32(out int n))
                {
                    return n;
                }

                if (json.ValueKind == JsonValueKind.String && int.TryParse(json.GetString(), out int s))
                {
                    return s;
                }

                return defaultValue;
            }

            return int.TryParse(value.ToString(), out int result) ? result : defaultValue;
        }

        private static bool GetBoolValue(Dictionary<string, object?> dic, string key, bool defaultValue)
        {
            if (!dic.TryGetValue(key, out object? value) || value == null)
            {
                return defaultValue;
            }

            if (value is JsonElement json)
            {
                if (json.ValueKind == JsonValueKind.True)
                {
                    return true;
                }
                if (json.ValueKind == JsonValueKind.False)
                {
                    return false;
                }
                if (json.ValueKind == JsonValueKind.String && bool.TryParse(json.GetString(), out bool b))
                {
                    return b;
                }
                return defaultValue;
            }

            return bool.TryParse(value.ToString(), out bool result) ? result : defaultValue;
        }

        private static string ResolvePath(string path, string baseDir)
        {
            if (Path.IsPathRooted(path))
            {
                return path;
            }

            return Path.GetFullPath(Path.Combine(baseDir, path));
        }

        private static List<Dictionary<string, object?>> ReadScript(string path)
        {
            if (!File.Exists(path))
            {
                return new List<Dictionary<string, object?>>();
            }

            string content = File.ReadAllText(path);
            return JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(content, JsonOptions) ?? new List<Dictionary<string, object?>>();
        }

        private static List<Dictionary<string, string>> ReadInputRows(string path)
        {
            if (!File.Exists(path))
            {
                return new List<Dictionary<string, string>>();
            }

            string extension = Path.GetExtension(path).ToLowerInvariant();
            return extension == ".csv" ? ReadCsvRows(path) : ReadExcelRows(path);
        }

        private static List<Dictionary<string, string>> ReadCsvRows(string path)
        {
            var rows = new List<Dictionary<string, string>>();
            string[] lines = File.ReadAllLines(path);
            if (lines.Length == 0)
            {
                return rows;
            }

            string[] headers = lines[0].Split(',').Select(x => x.Trim().ToLowerInvariant()).ToArray();
            for (int i = 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                {
                    continue;
                }

                string[] values = lines[i].Split(',');
                var item = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                for (int c = 0; c < headers.Length; c++)
                {
                    item[headers[c]] = c < values.Length ? values[c].Trim() : string.Empty;
                }
                rows.Add(item);
            }
            return rows;
        }

        private static List<Dictionary<string, string>> ReadExcelRows(string path)
        {
            var rows = new List<Dictionary<string, string>>();
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (var reader = ExcelReaderFactory.CreateReader(stream))
                {
                    var conf = new ExcelDataSetConfiguration
                    {
                        ConfigureDataTable = _ => new ExcelDataTableConfiguration
                        {
                            UseHeaderRow = false
                        }
                    };

                    DataSet result = reader.AsDataSet(conf);
                    if (result.Tables.Count == 0)
                    {
                        return rows;
                    }
                    DataTable table = result.Tables[0];
                    if (table.Rows.Count == 0)
                    {
                        return rows;
                    }
                    var headers = new List<string>();
                    foreach (object? value in table.Rows[0].ItemArray)
                    {
                        string header = value?.ToString()?.Trim().ToLowerInvariant() ?? string.Empty;
                        headers.Add(header);
                    }

                    for (int rowIndex = 1; rowIndex < table.Rows.Count; rowIndex++)
                    {
                        var item = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        for (int colIndex = 0; colIndex < headers.Count; colIndex++)
                        {
                            string header = headers[colIndex];
                            if (string.IsNullOrWhiteSpace(header))
                            {
                                continue;
                            }

                            item[header] = colIndex < table.Columns.Count
                                ? table.Rows[rowIndex][colIndex]?.ToString()?.Trim() ?? string.Empty
                                : string.Empty;
                        }

                        rows.Add(item);
                    }
                }
            }

            return rows;
        }

        private static JsonRunResult ReadResultIfExists(string fixedResultPath)
        {
            if (!File.Exists(fixedResultPath))
            {
                return new JsonRunResult();
            }

            string content = File.ReadAllText(fixedResultPath);
            return JsonSerializer.Deserialize<JsonRunResult>(content, JsonOptions) ?? new JsonRunResult();
        }

        private class JsonRunResult
        {
            [JsonPropertyName("thoi_gian")]
            public string ThoiGian { get; set; } = string.Empty;

            [JsonPropertyName("status")]
            public string Status { get; set; } = string.Empty;

            [JsonPropertyName("so_luong_succes")]
            public string SoLuongSucces { get; set; } = "0";

            [JsonPropertyName("so_luong_fail")]
            public string SoLuongFail { get; set; } = "0";

            [JsonPropertyName("header_da_chay")]
            public bool HeaderDaChay { get; set; }

            [JsonPropertyName("data")]
            public List<JsonRunItem> Data { get; set; } = new List<JsonRunItem>();
        }

        private class JsonRunItem
        {
            [JsonPropertyName("id")]
            public string Id { get; set; } = string.Empty;

            [JsonPropertyName("stt")]
            public string Stt { get; set; } = string.Empty;

            [JsonPropertyName("lan_chay")]
            public int LanChay { get; set; }

            [JsonPropertyName("status")]
            public string Status { get; set; } = string.Empty;

            [JsonPropertyName("message")]
            public string Message { get; set; } = string.Empty;

            [JsonPropertyName("message_before")]
            public string MessageBefore { get; set; } = string.Empty;
        }
    }
}
