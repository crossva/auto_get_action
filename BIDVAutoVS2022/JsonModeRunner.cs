using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using ExcelDataReader;
using OpenQA.Selenium;
using OpenQA.Selenium.Interactions;
using OpenQA.Selenium.Support.UI;

namespace BIDVAutoVS2022
{
    internal static class JsonModeRunner
    {
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNameCaseInsensitive = true
        };
        private static List<int>? _lastIframePath;

        public static void RunFromConfig()
        {
            _lastIframePath = null;
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

            IWebDriver? driverGC = null;
            Actions? actions = null;
            bool headerDone = headerSteps.Count == 0;
            var newItems = new List<JsonRunItem>();
            int success = 0;
            int fail = 0;

            try
            {
                driverGC = Program.GetWebDriver(isBrowseChrome, folderDownloadCur, version, versionFirerfox, onlineVersion, "0", "0", cheDoChayNheNhat, tempProfile);
                driverGC.Manage().Window.Maximize();
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
                        Logger.LogError($"[JSON CASE ERROR] STT={stt}", ex);
                        newItems.Add(new JsonRunItem
                        {
                            Id = stt,
                            Stt = stt,
                            LanChay = 1,
                            Status = "error",
                            Message = ex.ToString(),
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
                //WaitByInTime(inMs);

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
                    string replaceValue = ResolveInputValue(GetStringValue(step, "replace_value", ""), rowValues);
                    bool isClick = GetBoolValueFlexible(step, "is_click", false);
                    bool isClickAc = GetBoolValueFlexible(step, "is_click_ac", false);
                    bool isClickRow = GetBoolValueFlexible(step, "is_click_row", false);
                    string sel_id = GetStringValue(step, "sel_id", "");

                    Logger.LogInfo($"[JSON STEP] name={stepName}; type_by={typeBy}; s_value={selector}; input_value={inputValue}; begin={beginMs}; in={inMs}; end={endMs}");
                    ExecuteUiStep(rowValues, driverGC, actions, typeBy, selector, sel_id, inputValue, replaceValue, inMs, isClick, isClickAc, isClickRow);
                }

                SleepMs(endMs);
            }
        }

        private static void ExecuteUiStep(Dictionary<string, string> rowValues, IWebDriver driverGC, Actions actions, string typeBy, string selector, string sel_id, string inputValue, string replaceValue, int inMs, bool isClick, bool isClickAc, bool isClickRow)
        {
            if (string.Equals(typeBy, "url", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.IsNullOrWhiteSpace(selector))
                {
                    driverGC.Navigate().GoToUrl(selector);
                    _lastIframePath = null;
                }
                return;
            }

            if (string.Equals(typeBy, "switch_to_default", StringComparison.OrdinalIgnoreCase))
            {
                driverGC.SwitchTo().DefaultContent();
                _lastIframePath = null;
                return;
            }

            if (string.Equals(typeBy, "detail_hd", StringComparison.OrdinalIgnoreCase))
            {
                ExecuteDetailHoaDonStep(driverGC, rowValues, inMs);
                return;
            }

            if (string.Equals(typeBy, "sel", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.IsNullOrWhiteSpace(replaceValue))
                {
                    WaitAndFindElement(driverGC, By.XPath(selector), inMs);
                    SetSelect2Value(driverGC, replaceValue, inputValue, inMs);
                }
                else
                {
                    WaitAndFindElement(driverGC, By.Id(sel_id), inMs);
                    SetSelect2Value(driverGC, sel_id, inputValue, inMs);
                }
                return;
            }

            if (isClickRow)
            {
                string transactionNo = (GetRowValue(rowValues, "so_giao_dich") ?? string.Empty).Trim();
                if (!string.IsNullOrWhiteSpace(transactionNo))
                {
                    bool clickedByTransaction = ClickRowByTransactionNo(driverGC, transactionNo);
                    if (!clickedByTransaction)
                    {
                        throw new Exception($"Không tìm thấy dòng có Số chứng từ/Số giao dịch = '{transactionNo}' trong grid.");
                    }
                }
                else
                {
                    decimal target = ParseMoneyRequired(GetRowValue(rowValues, "so_tien"), "so_tien", "vn");
                    bool clickedByMoney = ClickRowByMoney(driverGC, target);
                    if (!clickedByMoney)
                    {
                        throw new Exception($"Không tìm thấy dòng có Số tiền = {target.ToString(CultureInfo.InvariantCulture)} trong grid.");
                    }
                }
                return;
            }

            By by = BuildBy(typeBy, selector);
            IWebElement element = WaitAndFindElement(driverGC, by, inMs);

            if (!string.IsNullOrWhiteSpace(inputValue) && !string.Equals(inputValue, "None", StringComparison.OrdinalIgnoreCase))
            {
                element.Clear();
                SleepMs(1000);
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

        private static void ExecuteDetailHoaDonStep(IWebDriver driverGC, Dictionary<string, string> rowValues, int inMs)
        {
            int rowIndex = GetRowIndexFromStt(rowValues);

            string productValue = GetRowValue(rowValues, "san_pham");
            string customerTypeValue = GetRowValue(rowValues, "pk_kh");
            string matHang = GetRowValue(rowValues, "mat_hang");
            string tenCb = GetRowValue(rowValues, "ten_cb");
            bool isQuaTang = IsOne(GetRowValue(rowValues, "is_qua_tang"));
            bool foundAnyDetailRow = false;
            for (int detailIndex = 0; ; detailIndex++)
            {
                string productSelectId = $"singleselect-InvoiceIn:TableInvoiceIn:expenses[{detailIndex}]:expenseTbl:TooltipProduct[0]:product";
                string customerSelectId = $"singleselect-InvoiceIn:TableInvoiceIn:expenses[{detailIndex}]:expenseTbl:TooltipCustomerType[0]:customerType";

                if (!ElementExistsById(driverGC, productSelectId) || !ElementExistsById(driverGC, customerSelectId))
                {
                    break;
                }
                foundAnyDetailRow = true;

                if (detailIndex >= 1)
                {
                    ScrollToElementById(driverGC, productSelectId, inMs);
                }

                SelectDropdownByValue(driverGC, productSelectId, productValue, inMs);
                SelectDropdownByValue(driverGC, customerSelectId, customerTypeValue, inMs);

                decimal expenseAmount = ReadDecimalInputById(driverGC, $"decimal-input-InvoiceIn:TableInvoiceIn:expenses[{detailIndex}]:expenseTbl:Tooltip1[0]:expenseAmount", inMs);
                decimal taxAmount = ReadDecimalInputById(driverGC, $"decimal-input-InvoiceIn:TableInvoiceIn:expenses[{detailIndex}]:expenseTbl:Tooltip2[0]:taxAmount", inMs);
                decimal expenseTaxAmount = expenseAmount + taxAmount;

                int thueSuat = expenseAmount <= 0 ? 0 : (int)Math.Round((taxAmount / expenseAmount) * 100m);

                string detailBtnId = $"icon-button-InvoiceIn:TableInvoiceIn:invoiceDetail[{detailIndex}]:Tooltip1:Icon5";
                WaitAndFindElement(driverGC, By.Id(detailBtnId), inMs).Click();

                SetInputById(driverGC, "decimal-input-ViewInvoiceInDetail:revenueNoTax", FormatDecimal(expenseAmount), inMs);
                SetInputById(driverGC, "decimal-input-ViewInvoiceInDetail:invoiceTaxAmount", FormatDecimal(taxAmount), inMs);
                SetInputById(driverGC, "decimal-input-ViewInvoiceInDetail:totalInvoicePayment", FormatDecimal(expenseTaxAmount), inMs);
                SetInputById(driverGC, "text-input-ViewInvoiceInDetail:itemName", matHang, inMs);
                SetInputById(driverGC, "text-input-ViewInvoiceInDetail:buyer", tenCb, inMs);
                string buton_xacnhan = $"/html/body/div[1]/div/div[4]/div/div/div/div[3]/div/div/div[1]/div/div/div/div[2]/div/div[2]/div/div[1]/button";
                WaitAndFindElement(driverGC, By.XPath(buton_xacnhan), inMs).Click();
                SleepMs(10000);
                if (isQuaTang)
                {
                    string giftBtnId = $"icon-button-InvoiceIn:TableInvoiceIn:goodGifts[{detailIndex}]:Icon3";
                    WaitAndFindElement(driverGC, By.Id(giftBtnId), inMs).Click();

                    WaitAndFindElement(driverGC, By.Id("radiogroup-item-input-TypeGiftedGoods[1]"), inMs).Click();
                    SetInputById(driverGC, "decimal-input-GiftGoods:CommodityValue", FormatDecimal(expenseAmount), inMs);
                    SetInputById(driverGC, "decimal-input-GiftGoods:TaxAmount", FormatDecimal(taxAmount), inMs);
                    SetInputById(driverGC, "text-input-GiftGoods:goodsName", matHang, inMs);
                    SelectTaxType(driverGC, "singleselect-GiftGoods:taxType", thueSuat, inMs);
                    WaitAndFindElement(driverGC, By.Id("button-button-Button13"), inMs).Click();
                }
                SleepMs(8000);
            }

            if (!foundAnyDetailRow)
            {
                throw new Exception($"Không tìm thấy dòng chi tiết nào trong expenseTbl của hóa đơn STT={rowIndex + 1}.");
            }
        }

        private static int GetRowIndexFromStt(Dictionary<string, string> rowValues)
        {
            string stt = GetRowValue(rowValues, "stt");
            if (int.TryParse(stt, out int sttInt) && sttInt > 0)
            {
                return sttInt - 1;
            }

            return 0;
        }

        private static bool ElementExistsById(IWebDriver driverGC, string id)
        {
            return TryFindElementWithFrameMemory(driverGC, By.Id(id), 200, out _);
        }

        private static void SelectDropdownByValue(IWebDriver driverGC, string id, string value, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(id), inMs);
            ScrollToElement(driverGC, element);
            var select = new SelectElement(element);

            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            try
            {
                select.SelectByValue(value);
            }
            catch (Exception ex)
            {
                Logger.LogError($"[JSON SELECT FALLBACK] Không SelectByValue được với id={id}, value={value}. Chuyển sang tìm option thủ công.", ex);
                var matched = select.Options.FirstOrDefault(o => string.Equals(o.GetAttribute("value")?.Trim(), value.Trim(), StringComparison.OrdinalIgnoreCase));
                if (matched == null)
                {
                    throw;
                }
                matched.Click();
            }
        }

        private static void SelectTaxType(IWebDriver driverGC, string id, int thueSuat, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(id), inMs);
            var select = new SelectElement(element);
            string taxText = thueSuat.ToString(CultureInfo.InvariantCulture);

            try
            {
                select.SelectByValue(taxText);
                return;
            }
            catch (Exception ex)
            {
                Logger.LogError($"[JSON TAXTYPE FALLBACK] SelectByValue thất bại với id={id}, tax={taxText}.", ex);
            }

            var option = select.Options.FirstOrDefault(o =>
                string.Equals((o.GetAttribute("value") ?? string.Empty).Trim(), taxText, StringComparison.OrdinalIgnoreCase)
                || (o.Text ?? string.Empty).Trim().StartsWith(taxText + "%", StringComparison.OrdinalIgnoreCase)
                || (o.Text ?? string.Empty).Trim().StartsWith(taxText, StringComparison.OrdinalIgnoreCase));

            if (option == null)
            {
                throw new Exception($"Không chọn được thuế suất '{taxText}' tại combobox {id}.");
            }

            option.Click();
        }

        private static void SetSelect2Value(IWebDriver driverGC, string selectId, string value, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(selectId), inMs);
            ScrollToElement(driverGC, element);

            try
            {
                var select = new SelectElement(element);
                select.SelectByValue(value);

                ((IJavaScriptExecutor)driverGC).ExecuteScript(
                    "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
                    element
                );
            }
            catch (Exception ex)
            {
                Logger.LogError($"[JSON SELECT2 FALLBACK] SelectByValue thất bại với selectId={selectId}, value={value}.", ex);
                ((IJavaScriptExecutor)driverGC).ExecuteScript(@"
                    arguments[0].value = arguments[1];
                    arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                ", element, value);
            }
        }

        private static void SelectByValueOrPrefix(IWebElement element, string inputValue)
        {
            var select = new SelectElement(element);

            if (string.IsNullOrWhiteSpace(inputValue))
            {
                return;
            }

            try
            {
                select.SelectByValue(inputValue);
                return;
            }
            catch (Exception ex)
            {
                Logger.LogError($"[JSON SELECT FALLBACK] SelectByValue thất bại với input={inputValue}.", ex);
            }

            var option = select.Options.FirstOrDefault(o => (o.Text ?? string.Empty).StartsWith(inputValue, StringComparison.OrdinalIgnoreCase));
            if (option == null)
            {
                throw new Exception($"Không tìm thấy option bắt đầu bằng '{inputValue}'.");
            }
            option.Click();
        }

        private static void SetInputById(IWebDriver driverGC, string id, string value, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(id), inMs);
            if (!string.IsNullOrWhiteSpace(value))
            {
                SleepMs(1000);
                element.Click();
                element.SendKeys(Keys.Control + "a");
                SleepMs(500);
                element.SendKeys(Keys.Delete);
                SleepMs(500);
                element.SendKeys(value);
            }
        }

        private static decimal ReadDecimalInputById(IWebDriver driverGC, string id, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(id), inMs);
            string raw = element.GetAttribute("value");
            if (string.IsNullOrWhiteSpace(raw))
            {
                raw = element.GetAttribute("data-uval");
            }
            if (string.IsNullOrWhiteSpace(raw))
            {
                raw = element.GetAttribute("data-lval");
            }

            return ParseMoneyRequired(raw ?? string.Empty, id, "en");
        }

        private static string FormatDecimal(decimal value)
        {
            if (decimal.Truncate(value) == value)
            {
                return decimal.Truncate(value).ToString(CultureInfo.InvariantCulture);
            }

            return value.ToString(CultureInfo.InvariantCulture);
        }

        private static bool IsOne(string raw)
        {
            return string.Equals((raw ?? string.Empty).Trim(), "1", StringComparison.OrdinalIgnoreCase);
        }

        private static string GetRowValue(Dictionary<string, string> rowValues, string key)
        {
            return rowValues.TryGetValue(key, out string? value) ? value ?? string.Empty : string.Empty;
        }

        private static decimal ParseMoneyRequired(string raw, string fieldName, string type_number)
        {
            decimal? parsed = ParseMoney_vn(raw);
            if (type_number == "vn")
            {
                parsed = ParseMoney_vn(raw);
            }
            else
            {
                parsed = ParseMoney_en(raw);
            }
            if (!parsed.HasValue)
            {
                throw new Exception($"Không đọc được giá trị số tại '{fieldName}'. Giá trị nhận được: '{raw}'.");
            }
            return parsed.Value;
        }


        private static IWebElement WaitAndFindElement(IWebDriver driverGC, By by, int timeoutMs = 5000)
        {
            int effectiveTimeout = timeoutMs > 0 ? timeoutMs : 5000;
            if (TryFindElementWithFrameMemory(driverGC, by, effectiveTimeout, out IWebElement? found))
            {
                return found!;
            }

            throw new NoSuchElementException($"Không tìm thấy phần tử '{by}' sau {effectiveTimeout}ms (đã duyệt qua iframe).");
        }

        private static bool TryFindElementWithFrameMemory(IWebDriver driverGC, By by, int timeoutMs, out IWebElement? foundElement)
        {
            foundElement = null;
            int effectiveTimeout = timeoutMs > 0 ? timeoutMs : 5000;
            Stopwatch sw = Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < effectiveTimeout)
            {
                if (TryFindInRememberedIframe(driverGC, by, out foundElement))
                {
                    return true;
                }

                if (TryFindInAllIframes(driverGC, by, out foundElement))
                {
                    return true;
                }

                Thread.Sleep(250);
            }

            return false;
        }

        private static bool TryFindInRememberedIframe(IWebDriver driverGC, By by, out IWebElement? foundElement)
        {
            foundElement = null;
            if (!SwitchToRememberedIframe(driverGC))
            {
                return false;
            }

            return TryFindInCurrentContext(driverGC, by, out foundElement);
        }

        private static bool SwitchToRememberedIframe(IWebDriver driverGC)
        {
            driverGC.SwitchTo().DefaultContent();

            if (_lastIframePath == null || _lastIframePath.Count == 0)
            {
                return true;
            }

            foreach (int frameIndex in _lastIframePath)
            {
                var frames = driverGC.FindElements(By.CssSelector("iframe,frame"));
                if (frameIndex < 0 || frameIndex >= frames.Count)
                {
                    _lastIframePath = null;
                    driverGC.SwitchTo().DefaultContent();
                    return false;
                }

                try
                {
                    driverGC.SwitchTo().Frame(frames[frameIndex]);
                }
                catch (Exception ex)
                {
                    Logger.LogError($"[JSON IFRAME] Không switch được vào iframe index={frameIndex} theo path nhớ.", ex);
                    _lastIframePath = null;
                    driverGC.SwitchTo().DefaultContent();
                    return false;
                }
            }

            return true;
        }
        private static void SelectByValueWithChange(IWebDriver driverGC, string selectId, string value)
        {
            var element = driverGC.FindElement(By.Id(selectId));

            var select = new SelectElement(element);
            select.SelectByValue(value);

            ((IJavaScriptExecutor)driverGC).ExecuteScript(
                "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
                element
            );
        }
        private static bool TryFindInAllIframes(IWebDriver driverGC, By by, out IWebElement? foundElement)
        {
            driverGC.SwitchTo().DefaultContent();
            var path = new List<int>();
            bool found = TryFindInAllIframesRecursive(driverGC, by, path, out foundElement, out List<int>? foundPath);
            if (found)
            {
                _lastIframePath = foundPath;
                return true;
            }

            return false;
        }

        private static bool TryFindInAllIframesRecursive(IWebDriver driverGC, By by, List<int> currentPath, out IWebElement? foundElement, out List<int>? foundPath)
        {
            foundElement = null;
            foundPath = null;

            if (TryFindInCurrentContext(driverGC, by, out foundElement))
            {
                foundPath = new List<int>(currentPath);
                return true;
            }

            var frames = driverGC.FindElements(By.CssSelector("iframe,frame"));
            for (int i = 0; i < frames.Count; i++)
            {
                try
                {
                    driverGC.SwitchTo().Frame(frames[i]);
                }
                catch (Exception ex)
                {
                    Logger.LogError($"[JSON IFRAME] Không switch được vào iframe index={i} khi duyệt toàn bộ iframe.", ex);
                    continue;
                }

                currentPath.Add(i);
                if (TryFindInAllIframesRecursive(driverGC, by, currentPath, out foundElement, out foundPath))
                {
                    return true;
                }

                currentPath.RemoveAt(currentPath.Count - 1);
                driverGC.SwitchTo().ParentFrame();
            }

            return false;
        }

        private static bool TryFindInCurrentContext(IWebDriver driverGC, By by, out IWebElement? foundElement)
        {
            foundElement = null;
            try
            {
                var elements = driverGC.FindElements(by);
                if (elements.Count == 0)
                {
                    return false;
                }

                foundElement = elements[0];
                return true;
            }
            catch (Exception ex)
            {
                Logger.LogError($"[JSON FIND] Lỗi khi tìm element {by}.", ex);
                return false;
            }
        }

        private static void ScrollToElementById(IWebDriver driverGC, string id, int inMs)
        {
            IWebElement element = WaitAndFindElement(driverGC, By.Id(id), inMs);
            ScrollToElement(driverGC, element);
        }

        private static void ScrollToElement(IWebDriver driverGC, IWebElement element)
        {
            try
            {
                ((IJavaScriptExecutor)driverGC).ExecuteScript(
                    "arguments[0].scrollIntoView({block:'center', inline:'nearest'});",
                    element
                );
                Thread.Sleep(120);
            }
            catch (Exception ex)
            {
                Logger.LogError("[JSON SCROLL] Không thể scroll tới element.", ex);
            }
        }
        private static By BuildBy(string typeBy, string selector)
        {
            switch ((typeBy ?? string.Empty).Trim().ToLowerInvariant())
            {
                case "id":
                    return By.Id(selector);
                case "linktext":
                    return By.LinkText(selector);
                case "css":
                    return By.CssSelector(selector);
                case "name":
                    return By.Name(selector);
                case "btn":
                case "path":
                case "data":
                case "xp_hidden":
                default:
                    return By.XPath(selector);
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
                            ? NormalizeExcelCellValue(table.Rows[rowIndex][colIndex])
                            : string.Empty;
                    }

                    rows.Add(item);
                }
            }

            return rows;
        }

        private static string NormalizeExcelCellValue(object? rawValue)
        {
            if (rawValue == null || rawValue == DBNull.Value)
            {
                return string.Empty;
            }

            if (rawValue is DateTime dt)
            {
                return dt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
            }

            string text = rawValue.ToString()?.Trim() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(text))
            {
                return string.Empty;
            }

            if (TryFormatDateString(text, out string normalizedDate))
            {
                return normalizedDate;
            }

            return text;
        }

        private static bool TryFormatDateString(string text, out string normalizedDate)
        {
            normalizedDate = string.Empty;

            // Chỉ xử lý các chuỗi có mẫu ngày để tránh parse nhầm dữ liệu thường.
            if (!Regex.IsMatch(text, @"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"))
            {
                return false;
            }

            string[] formats = new[]
            {
                "d/M/yyyy", "dd/MM/yyyy", "d/M/yy", "dd/MM/yy",
                "d-M-yyyy", "dd-MM-yyyy", "d-M-yy", "dd-MM-yy",
                "d/M/yyyy H:m:s", "dd/MM/yyyy HH:mm:ss", "d/M/yyyy HH:mm:ss",
                "d-M-yyyy H:m:s", "dd-MM-yyyy HH:mm:ss", "d-M-yyyy HH:mm:ss"
            };

            if (DateTime.TryParseExact(text, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dt)
                || DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)
                || DateTime.TryParse(text, new CultureInfo("vi-VN"), DateTimeStyles.None, out dt))
            {
                normalizedDate = dt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                return true;
            }

            return false;
        }

        private static bool ClickRowByMoney(IWebDriver driver, decimal targetMoney, int maxScrollTries = 60)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20));
            wait.Until(d => d.FindElement(By.CssSelector("div.ui-grid-render-container-body")));

            var headerCells = wait.Until(d => d.FindElements(By.CssSelector(".ui-grid-header-cell .ui-grid-cell-contents span.ng-binding")));
            int moneyColIndex = -1;

            for (int i = 0; i < headerCells.Count; i++)
            {
                string title = (headerCells[i].Text ?? string.Empty).Trim();
                if (string.Equals(title, "Số tiền", StringComparison.OrdinalIgnoreCase))
                {
                    moneyColIndex = i - 1;
                    break;
                }
            }

            if (moneyColIndex < 0)
            {
                throw new Exception("Không tìm thấy cột 'Số tiền' trong header ui-grid.");
            }

            var viewport = wait.Until(d => d.FindElement(By.CssSelector(".ui-grid-render-container-body")));

            for (int scrollTry = 0; scrollTry < maxScrollTries; scrollTry++)
            {
                var rows = driver.FindElements(By.CssSelector(".ui-grid-render-container-body .ui-grid-row"));

                foreach (var row in rows)
                {
                    var cells = row.FindElements(By.CssSelector(".ui-grid-cell"));
                    if (cells.Count <= moneyColIndex)
                    {
                        continue;
                    }

                    string moneyText = cells[moneyColIndex].Text?.Trim() ?? string.Empty;
                    decimal? moneyVal = ParseMoney_vn(moneyText);
                    if (moneyVal.HasValue && moneyVal.Value == targetMoney)
                    {
                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView({block:'center'});", row);
                        cells[moneyColIndex].Click();
                        return true;
                    }
                }

                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", viewport);
                Thread.Sleep(250);
            }

            return false;
        }

        private static bool ClickRowByTransactionNo(IWebDriver driver, string targetTransactionNo, int maxScrollTries = 60)
        {
            if (string.IsNullOrWhiteSpace(targetTransactionNo))
            {
                return false;
            }

            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20));
            wait.Until(d => d.FindElement(By.CssSelector("div.ui-grid-render-container-body")));

            var headerCells = wait.Until(d => d.FindElements(By.CssSelector(".ui-grid-header-cell .ui-grid-cell-contents span.ng-binding")));
            int transactionColIndex = -1;

            for (int i = 0; i < headerCells.Count; i++)
            {
                string title = NormalizeGridText(headerCells[i].Text);
                if (string.Equals(title, "Số chứng từ", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(title, "Số giao dịch", StringComparison.OrdinalIgnoreCase))
                {
                    transactionColIndex = i - 1;
                    break;
                }
            }

            if (transactionColIndex < 0)
            {
                throw new Exception("Không tìm thấy cột 'Số chứng từ' hoặc 'Số giao dịch' trong header ui-grid.");
            }

            var viewport = wait.Until(d => d.FindElement(By.CssSelector(".ui-grid-render-container-body")));
            string normalizedTarget = NormalizeGridText(targetTransactionNo);
            string compactTarget = Regex.Replace(normalizedTarget, @"\s+", "");

            for (int scrollTry = 0; scrollTry < maxScrollTries; scrollTry++)
            {
                var rows = driver.FindElements(By.CssSelector(".ui-grid-render-container-body .ui-grid-row"));

                foreach (var row in rows)
                {
                    var cells = row.FindElements(By.CssSelector(".ui-grid-cell"));
                    if (cells.Count <= transactionColIndex)
                    {
                        continue;
                    }

                    string transactionText = NormalizeGridText(cells[transactionColIndex].Text);
                    string compactTransactionText = Regex.Replace(transactionText, @"\s+", "");

                    if (string.Equals(transactionText, normalizedTarget, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(compactTransactionText, compactTarget, StringComparison.OrdinalIgnoreCase))
                    {
                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView({block:'center'});", row);
                        cells[transactionColIndex].Click();
                        return true;
                    }
                }

                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", viewport);
                Thread.Sleep(250);
            }

            return false;
        }

        private static string NormalizeGridText(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return string.Empty;
            }

            string cleaned = raw.Trim().Replace("\u00A0", " ");
            return Regex.Replace(cleaned, @"\s+", " ");
        }

        private static decimal? ParseMoney_vn(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            string cleaned = raw.Trim().Replace("\u00A0", " ");
            cleaned = Regex.Replace(cleaned, @"[^\d\.,\-]", "");
            if (string.IsNullOrWhiteSpace(cleaned))
            {
                return null;
            }

            cleaned = cleaned.Replace(",", string.Empty).Replace(".", string.Empty);
            if (decimal.TryParse(cleaned, NumberStyles.Number | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out decimal val))
            {
                return val;
            }

            return null;
        }
        private static decimal? ParseMoney_en(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return null;

            string cleaned = raw.Trim().Replace("\u00A0", "");

            // chỉ giữ số + . + , + -
            cleaned = Regex.Replace(cleaned, @"[^\d\.,\-]", "");

            if (string.IsNullOrWhiteSpace(cleaned))
                return null;

            // xử lý format VN: 3.009.260,00
            if (cleaned.Contains(",") && cleaned.Contains("."))
            {
                cleaned = cleaned.Replace(".", ""); // bỏ thousand
                cleaned = cleaned.Replace(",", "."); // đổi decimal
            }
            else if (cleaned.Contains(",") && !cleaned.Contains("."))
            {
                // trường hợp 3000,50
                cleaned = cleaned.Replace(",", ".");
            }
            else
            {
                // trường hợp 3000.50 hoặc 3000
            }

            if (decimal.TryParse(
                cleaned,
                NumberStyles.Number | NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out decimal val))
            {
                return val;
            }

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
