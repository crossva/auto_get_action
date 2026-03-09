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
using OpenQA.Selenium;
using OpenQA.Selenium.Interactions;
using OpenQA.Selenium.Support.UI;
using System.Text.RegularExpressions;

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

            string pathDownload = ConfigurationManager.AppSettings["path_download"] ?? Path.Combine(baseDir, "download");
            string version = ConfigurationManager.AppSettings["version"] ?? "v0.36.0";
            string onlineVersion = ConfigurationManager.AppSettings["online_version"] ?? "0";
            string isBrowseChrome = ConfigurationManager.AppSettings["is_browse_chrome"] ?? "0";
            string versionFirerfox = ConfigurationManager.AppSettings["version_firerfox"] ?? "v0.36.0";
            string cheDoChayNheNhat = ConfigurationManager.AppSettings["che_do_chay_nhe_nhat"] ?? "0";
            string quitBrowse = ConfigurationManager.AppSettings["quit_browse"] ?? "1";

            string folderDownloadCur = Path.Combine(pathDownload, DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss", CultureInfo.InvariantCulture));
            Directory.CreateDirectory(folderDownloadCur);
            string tempProfile = Path.Combine(baseDir, "temp", "json_profile");
            Directory.CreateDirectory(tempProfile);

            IWebDriver driverGC = null;
            Actions actions = null;

            bool headerDone = headerSteps.Count == 0;
            var newItems = new List<JsonRunItem>();
            int success = 0;
            int fail = 0;

            try
            {
                driverGC = Program.GetWebDriver(isBrowseChrome, folderDownloadCur, version, versionFirerfox, onlineVersion, "0", "0", cheDoChayNheNhat, tempProfile);
                //driverGC.Manage().Window.Maximize();
                actions = new Actions(driverGC);


                if (headerSteps.Count > 0)
                {
                    ExecuteSteps(headerSteps, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), Path.GetDirectoryName(detailScriptPath) ?? baseDir, driverGC, actions);
                    headerDone = true;
                }

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
                        ExecuteSteps(detailSteps, row, Path.GetDirectoryName(detailScriptPath) ?? baseDir, driverGC, actions);
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
            }
            finally
            {
                if (quitBrowse == "1" && driverGC != null)
                {
                    driverGC.Quit();
                    driverGC.Dispose();
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

        private static void ExecuteSteps(List<Dictionary<string, object?>> steps, Dictionary<string, string> rowValues, string scriptDirectory, IWebDriver driverGC, Actions actions)
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
                        ExecuteSteps(loopSteps, rowValues, Path.GetDirectoryName(loopPath) ?? scriptDirectory, driverGC, actions);
                    }
                }
                else
                {
                    string inputValue = ResolveInputValue(GetStringValue(step, "input_value", ""), rowValues);
                    string selector = ResolveInputValue(GetStringValue(step, "s_value", ""), rowValues);
                    bool isClick = GetBoolValueFlexible(step, "is_click", false);
                    bool isClickAc = GetBoolValueFlexible(step, "is_click_ac", false);
                    bool isClickGrid = GetBoolValueFlexible(step, "is_click_row", false);
                    Logger.LogInfo($"[JSON STEP] name={stepName}; type_by={typeBy}; s_value={selector}; input_value={inputValue}; begin={beginMs}; in={inMs}; end={endMs}");
                    ExecuteUiStep(rowValues, driverGC, actions, typeBy, selector, inputValue, inMs, isClick, isClickAc, isClickGrid);
                }

                SleepMs(endMs);
            }
        }

        private static void ExecuteUiStep(Dictionary<string, string> rowValues, IWebDriver driverGC, Actions actions, string typeBy, string selector, string inputValue, int inMs, bool isClick, bool isClickAc, bool isClickGrid)
        {
            if (string.Equals(typeBy, "url", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.IsNullOrWhiteSpace(selector))
                {
                    driverGC.Navigate().GoToUrl(selector);
                }
                return;
            }

            if (string.Equals(typeBy, "switch_to_default", StringComparison.OrdinalIgnoreCase))
            {
                driverGC.SwitchTo().DefaultContent();
                return;
            }
            if (isClickGrid)
            {
                decimal? targetNullable = ParseMoney(rowValues["so_tien"]);

                if (!targetNullable.HasValue)
                    throw new Exception($"Giá trị Số tiền không hợp lệ: '{inputValue}'");

                decimal target = targetNullable.Value;
                bool clicked = ClickRowByMoney(driverGC, target);
            }
            else
            {
                By by = BuildBy(typeBy, selector);
                IWebElement element = WaitAndFindElement(driverGC, by, inMs);
                if (!string.IsNullOrWhiteSpace(inputValue) && !string.Equals(inputValue, "None", StringComparison.OrdinalIgnoreCase))
                {
                    element.Clear();
                    element.SendKeys(inputValue);
                }

                if (isClickAc)
                {
                    actions.MoveToElement(element).Click().Perform();
                }
                else if (isClick || string.IsNullOrWhiteSpace(inputValue))
                {
                    element.Click();
                }
            }

        }

        private static IWebElement WaitAndFindElement(IWebDriver driverGC, By by, int inMs)
        {
            int timeoutMs = inMs > 0 ? inMs : 5000;
            var wait = new WebDriverWait(driverGC, TimeSpan.FromMilliseconds(timeoutMs));
            wait.PollingInterval = TimeSpan.FromMilliseconds(250);
            wait.IgnoreExceptionTypes(typeof(NoSuchElementException), typeof(StaleElementReferenceException));
            return wait.Until(d => d.FindElement(by));
        }

        private static By BuildBy(string typeBy, string selector)
        {
            switch (typeBy.Trim().ToLowerInvariant())
            {
                case "id":
                    return By.Id(selector);
                case "linktext":
                    return By.LinkText(selector);
                case "css":
                    return By.CssSelector(selector);
                case "name":
                    return By.Name(selector);
                case "path":
                case "data":
                case "xp_hidden":
                default:
                    return By.XPath(selector);
            }
        }

        public static void SelectByPrefix(IWebDriver driver, string id, string prefix)
        {
            var select = new SelectElement(driver.FindElement(By.Id(id)));

            foreach (var option in select.Options)
            {
                if (option.Text.StartsWith(prefix))
                {
                    option.Click();
                    return;
                }
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

        private static bool GetBoolValueFlexible(Dictionary<string, object?> dic, string key, bool defaultValue)
        {
            string value = GetStringValue(dic, key, defaultValue ? "true" : "false");
            if (bool.TryParse(value, out bool boolValue))
            {
                return boolValue;
            }

            if (value == "1")
            {
                return true;
            }

            if (value == "0")
            {
                return false;
            }

            return defaultValue;
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
        public static bool ClickRowByMoney(IWebDriver driver, decimal targetMoney, int maxScrollTries = 60)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20));

            // 1) Xác định container grid (lấy container body)
            var gridContainer = wait.Until(d => d.FindElement(By.CssSelector("div.ui-grid-render-container-body")));

            // 2) Tìm index cột "Số tiền" dựa trên header text
            var headerCells = wait.Until(d => d.FindElements(By.CssSelector(".ui-grid-header-cell .ui-grid-cell-contents span.ng-binding")));
            int moneyColIndex = -1;

            for (int i = 0; i < headerCells.Count; i++)
            {
                var title = (headerCells[i].Text ?? "").Trim();
                if (string.Equals(title, "Số tiền", StringComparison.OrdinalIgnoreCase))
                {
                    moneyColIndex = i-1;
                    break;
                }
            }

            if (moneyColIndex < 0)
                throw new Exception("Không tìm thấy cột 'Số tiền' trong header ui-grid.");

            // 3) Lấy viewport body để scroll
            // ui-grid thường có viewport: .ui-grid-viewport / .ui-grid-render-container-body
            var viewport = wait.Until(d => d.FindElement(By.CssSelector(".ui-grid-render-container-body")));

            // 4) Duyệt theo từng “page render” + scroll xuống
            // NOTE: ui-grid row thường có class .ui-grid-row
            for (int scrollTry = 0; scrollTry < maxScrollTries; scrollTry++)
            {
                var rows = driver.FindElements(By.CssSelector(".ui-grid-render-container-body .ui-grid-row"));

                foreach (var row in rows)
                {
                    // Các cell trong row
                    var cells = row.FindElements(By.CssSelector(".ui-grid-cell"));

                    if (cells == null || cells.Count <= moneyColIndex) continue;

                    // Lấy text cell "Số tiền"
                    var moneyText = cells[moneyColIndex].Text?.Trim() ?? "";
                    var moneyVal = ParseMoney(moneyText);

                    if (moneyVal.HasValue && moneyVal.Value == targetMoney)
                    {
                        // Scroll vào view rồi click
                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView({block:'center'});", row);

                        // Click cả row (hoặc click cell số tiền)
                        // row.Click();
                        cells[moneyColIndex].Click();

                        return true;
                    }
                }

                // 5) Chưa thấy -> scroll xuống thêm (virtual scroll)
                // Scroll viewport bằng JS để ui-grid render thêm rows
                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", viewport);

                // Đợi chút để ui-grid render lại
                Thread.Sleep(250);
            }

            return false;
        }
        private static decimal? ParseMoney(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;

            // Bỏ ký tự không phải số, dấu phẩy, dấu chấm, dấu âm
            raw = raw.Trim();

            // UI hay có NBSP
            raw = raw.Replace("\u00A0", " ");

            // Giữ lại chỉ digits + , .
            var cleaned = Regex.Replace(raw, @"[^\d\.,\-]", "");

            if (string.IsNullOrWhiteSpace(cleaned)) return null;

            // Trường hợp VN: 1.234.567 (dấu . là phân tách nghìn)
            // Trường hợp EN: 1,234,567 (dấu , là phân tách nghìn)
            // Ta xử lý: nếu có cả '.' và ',' thì đoán ',' là decimal (hiếm) -> bỏ nghìn.
            // Ở thực tế tiền thường không có phần thập phân => chỉ remove thousand separators.
            cleaned = cleaned.Replace(",", "").Replace(".", "");

            if (decimal.TryParse(cleaned, NumberStyles.Number | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var val))
                return val;

            return null;
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
