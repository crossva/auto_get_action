using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenQA.Selenium;
using OpenQA.Selenium.Firefox;
using OpenQA.Selenium.Support.UI;
using SeleniumExtras.WaitHelpers;
using System.Threading;
using System.Configuration;
using System.Data;
using OpenQA.Selenium.Remote;
using System.IO;
using OpenQA.Selenium.Interactions;
using WebDriverManager;
using WebDriverManager.DriverConfigs.Impl;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.BiDi.Communication;
using System.Runtime.InteropServices;
using OpenQA.Selenium.DevTools;
using System.Collections.Generic;
using System.Diagnostics;

namespace BIDVAutoVS2022
{
    class Program
    {

        static bool WaitIsElementFound(ref IWebDriver driverGC, string type_by, string s_value, string in_time_sleep)
        {
            int totalWait = Convert.ToInt32(Convert.ToInt32(in_time_sleep) / 5000);
            int i = 0;

            while (i < totalWait)
            {
                try
                {
                    By by;

                    // Xác định kiểu tìm kiếm
                    switch (type_by.ToLower())
                    {
                        case "id":
                            by = By.Id(s_value);
                            break;
                        case "path":
                        case "xp_hidden":
                        case "data": // dùng XPath
                            by = By.XPath(s_value);
                            break;
                        case "linktext":
                            by = By.LinkText(s_value);
                            break;
                        case "css":
                            by = By.CssSelector(s_value);
                            break;
                        default:
                            by = By.XPath(s_value);
                            break;
                    }

                    // Gọi hàm tự động quét iframe
                    IWebElement? element = FindElementWithAutoFrame(driverGC, by, 5);

                    if (element != null)
                    {
                        // Đã tìm thấy element trong 1 trong các frame
                        return true;
                    }
                }
                catch (NoSuchElementException)
                {
                    // Không có element → chờ tiếp
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARN] Lỗi khi tìm element ({s_value}): {ex.Message}");
                }

                Thread.Sleep(500);
                i++;
            }

            // Không tìm thấy sau thời gian chờ
            driverGC.SwitchTo().DefaultContent();
            return false;
        }

        static bool WaitIsElementNotFound(ref IWebDriver driverGC, string type_by, string s_value, string in_time_sleep)
        {
            int i = 0;
            int n = Convert.ToInt32(Convert.ToInt32(in_time_sleep) / 500);
            while (i < n)
            {
                try
                {
                    if (type_by == "id")
                    {
                        driverGC.FindElement(By.Id(s_value));
                    }
                    else if (type_by == "path")
                    {
                        /// tìm giá trị // nếu có sẽ thay thế thành giá trị thì sẽ 
                        driverGC.FindElement(By.XPath(s_value));
                    }
                    else if (type_by == "data")
                    {
                        driverGC.FindElement(By.XPath(s_value));
                    }
                    else
                    {
                        driverGC.FindElement(By.XPath(s_value));
                    }
                    Thread.Sleep(500);
                }
                catch (Exception e)
                {
                    return true;
                }
                i = i + 1;
            }
            return false;
        }

        static string GetNewFile(string full_file_name_dl)
        {
            string sResult = full_file_name_dl;
            int i = 1;
            int n = 1000;
            int iext = full_file_name_dl.LastIndexOf(".");
            string file_name_not_ex = full_file_name_dl.Substring(0, iext);
            string s_extend = full_file_name_dl.Substring(iext + 1);
            bool cont = true;
            while (i < n && cont)
            {
                if (File.Exists(sResult))
                {
                    sResult = string.Format("{0} ({1}).{2}", file_name_not_ex, i, s_extend);
                }
                else
                {
                    cont = false;
                }
                i = i + 1;
            }
            return sResult;
        }

        static bool ActrionOneStep(ref IWebDriver driverGC, ref Actions actions, ref int step, ref string s_result_data,
            string type_by, string begin_time_sleep, string in_time_sleep, string end_time_sleep,
            string s_value, string order_by, string ods_import_auto_get_id,
            string server, int port, string database_name,
            string is_click, string input_value, string is_click_ac, string is_data, string sql_finish, string is_popup_download, string FolderDownloadCur)
        {
            bool bResult = true;
            string sql_execute = "";
            string note_ = "";
            IJavaScriptExecutor js = (IJavaScriptExecutor)driverGC;
            IWebElement curElement;
            if (type_by == "id")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, type_by, s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                //w.Until(ExpectedConditions.ElementExists(By.Id(s_value)));
                curElement = driverGC.FindElement(By.Id(s_value));
                if (is_click == "true")
                {
                    curElement.Click();
                    //js.ExecuteScript("arguments[0].click();", curElement);
                }
                    
                if (input_value != "")
                {
                    curElement.Clear();
                    curElement.SendKeys(input_value);
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "path")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, type_by, s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                //w.Until(ExpectedConditions.ElementExists(By.XPath(s_value)));
                string selection_value = "";
                if (input_value != "")
                {
                    int vt_select = input_value.IndexOf("select ");
                    if (vt_select >= 0)
                    {
                        selection_value = input_value.Substring(7);
                    }
                }
                curElement = driverGC.FindElement(By.XPath(s_value));
                if (is_click == "true")
                {
                    if (is_popup_download == "true")
                    {
                        string mainWindow = driverGC.CurrentWindowHandle;
                        WebDriverWait wait = new WebDriverWait(driverGC, TimeSpan.FromSeconds(Convert.ToInt32(in_time_sleep) / 1000));
                        js.ExecuteScript("arguments[0].click();", curElement);
                        int vt_select = s_value.IndexOf("option[");
                        if (vt_select >= 0)
                        {
                            curElement.Click();
                        }
                        else if (s_value.IndexOf("button") >= 0)
                        {
                            curElement.Click();
                        }
                        try
                        {
                            WebDriverWait shortWait = new WebDriverWait(driverGC, TimeSpan.FromSeconds(3));
                            shortWait.Until(d => d.WindowHandles.Count > 1);

                            // Nếu có popup thì lấy handle cuối
                            string popupWindow = driverGC.WindowHandles.Last();

                            // Chờ popup tự đóng (tối đa số giây định nghĩa)
                            WebDriverWait popupWait = new WebDriverWait(driverGC, TimeSpan.FromSeconds(Convert.ToInt32(in_time_sleep) / 1000));
                            popupWait.Until(d => d.WindowHandles.Count == 1);

                            // Quay về cửa sổ chính
                            driverGC.SwitchTo().Window(mainWindow);
                        }
                        catch (WebDriverTimeoutException)
                        {
                            // Không có popup → tiếp tục bình thường
                            driverGC.SwitchTo().Window(mainWindow);
                        }
                        //wait.Until(d => d.WindowHandles.Count > 1);
                        //string popupWindow = driverGC.WindowHandles.Last();
                        //wait.Until(d => d.WindowHandles.Count == 1);
                        //driverGC.SwitchTo().Window(mainWindow);
                        try
                        {
                            // chờ file tải xong
                            WaitForAllDownloadsComplete(FolderDownloadCur, 300);

                            // ghi log thành công
                            Console.WriteLine("Tải file hoàn tất.");
                        }
                        catch (Exception ex)
                        {
                            bResult = false;
                            Console.WriteLine("Lỗi tải file: " + ex.Message);
                            throw;
                        }
                        bResult = true;
                    }
                    else
                    {
                        //js.ExecuteScript("arguments[0].click();", curElement);
                        int vt_select = s_value.IndexOf("option[");
                        if (vt_select >= 0)
                        {
                            curElement.Click();
                        }
                        else if (s_value.IndexOf("button") >= 0)
                        {
                            WebDriverWait wait = new WebDriverWait(driverGC, TimeSpan.FromSeconds(10));
                            // Chờ overlay biến mất
                            wait.Until(ExpectedConditions.InvisibilityOfElementLocated(By.CssSelector(".clsViewerBlocker")));

                            IWebElement curElement1 = wait.Until(
                                ExpectedConditions.ElementToBeClickable(By.XPath(s_value))
                            );
                            // Sau đó click
                            //wait.Until(ExpectedConditions.ElementToBeClickable(By.XPath("//button[normalize-space()='Finish']"))).Click();
                            curElement1.Click();
                        }
                        else
                        {
                            js.ExecuteScript("arguments[0].click();", curElement);
                        }
                    }
                }
                else if (is_click_ac == "true")
                {
                    if (selection_value == "")
                    {
                        actions.MoveToElement(curElement).Click().Perform();
                    }
                    else
                    {
                        SelectElement statusId = new SelectElement(curElement);
                        statusId.SelectByValue(selection_value);
                    }

                }
                if (input_value != "" && selection_value == "")
                {
                    curElement.Clear();
                    if (input_value != "None")
                    {
                        curElement.SendKeys(input_value);
                    }    
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "linktext")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, type_by, s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                //w.Until(ExpectedConditions.ElementExists(By.XPath(s_value)));
                List<IWebElement> curElements = driverGC.FindElements(By.LinkText(s_value)).ToList();
                if (s_value == "Select all" && curElements.Count > 1)
                {
                    foreach (IWebElement curElement_in_list in curElements)
                    {
                        if (is_click == "true")
                        {
                            js.ExecuteScript("arguments[0].click();", curElement_in_list);
                        }
                        else if (is_click_ac == "true")
                        {
                            actions.MoveToElement(curElement_in_list).Click().Perform();
                        }
                        if (input_value != "")
                        {
                            curElement_in_list.SendKeys(input_value);
                        }
                    }
                }
                else
                {
                    curElement = driverGC.FindElement(By.LinkText(s_value));
                    if (is_click == "true")
                    {
                        js.ExecuteScript("arguments[0].click();", curElement);
                    }
                    else if (is_click_ac == "true")
                    {
                        actions.MoveToElement(curElement).Click().Perform();
                    }
                    if (input_value != "")
                    {
                        curElement.SendKeys(input_value);
                    }
                }

                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "not_found")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementNotFound(ref driverGC, type_by, s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do luôn luôn tồn tại element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            if (type_by == "id_hidden")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, "id", s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                curElement = driverGC.FindElement(By.Id(s_value));
                if (is_click == "true")
                {
                    js.ExecuteScript("arguments[0].click();", curElement);
                }
                else if (is_click_ac == "true")
                {
                    actions.MoveToElement(curElement).Click().Perform();
                }
                if (input_value != "")
                {
                    string script = "arguments[0].setAttribute('value', arguments[1]);";
                    js.ExecuteScript(script, curElement, input_value);
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "xp_hidden")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, "path", s_value, in_time_sleep))
                {
                    /// không tồn tại element nên không tiếp tục thực hiện nữa
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy element {1}", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    return false;
                }
                //w.Until(ExpectedConditions.ElementExists(By.XPath(s_value)));
                curElement = driverGC.FindElement(By.XPath(s_value));
                if (is_click == "true")
                {
                    curElement.Click();
                    //js.ExecuteScript("arguments[0].click();", curElement);
                }
                else if (is_click_ac == "true")
                {
                    actions.MoveToElement(curElement).Click().Perform();
                }
                if (input_value != "")
                {
                    string script = "arguments[0].setAttribute('value', arguments[1]);";
                    js.ExecuteScript(script, curElement, input_value);
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "frame")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                driverGC.SwitchTo().Frame(Convert.ToInt32(s_value));
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "noframe")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                driverGC.SwitchTo().DefaultContent();
                //driverGC.SwitchTo().Frame(0);
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "url")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                driverGC.Navigate().GoToUrl(s_value);
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "data")
            {
                Thread.Sleep(Convert.ToInt32(begin_time_sleep));
                if (!WaitIsElementFound(ref driverGC, type_by, s_value, in_time_sleep))
                {
                    if (is_data == "true")
                    {
                        /// Kiểm tra nếu có điều kiện sql tồn tại dữ liệu thì kiểm tra nếu có dữ liệu thì sẽ tự thoát luôn không cần kiểm tra có dữ liệu
                        sql_finish = sql_finish.Replace("para_datadate", input_value);
                        if (sql_finish != "")
                        {
                            DataTable data_exist = common_sql.GetData(sql_finish, server, port, database_name);
                            if (data_exist.Rows.Count > 0)
                            {
                                s_result_data = "Khong co du lieu trong ngay";
                                return true;
                            }
                        }
                        sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                        note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy dữ liệu", order_by);
                        sql_execute += string.Format(" note = '{0}'", note_);
                        sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                        common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                        return false;
                    }
                    else
                    {
                        s_result_data = "Khong co du lieu";
                        return true;
                    }

                }
                curElement = driverGC.FindElement(By.XPath(s_value));
                if (is_click == "true")
                {
                    js.ExecuteScript("arguments[0].click();", curElement);
                }
                else if (is_click_ac == "true")
                {
                    actions.MoveToElement(curElement).Click().Perform();
                }
                string data_check = curElement.GetAttribute("textContent");
                if (data_check != "")
                {
                    data_check = data_check.Trim('-');
                    string s_date_check = "";
                    if (input_value != "")
                    {
                        if (!data_check.Contains("/") && !data_check.Contains("-"))
                        {
                            s_date_check = ParseCustomDate(data_check);
                        }
                        else if (!data_check.Contains("/"))
                        {
                            s_date_check = data_check;
                        }    
                        else
                        {
                            string[] a_date = data_check.Split('/');
                            /// mặt định định dạng ngày là 'dd/mm/yyyy'
                            DateTime date_check = new DateTime(Convert.ToInt32(a_date[2]), Convert.ToInt32(a_date[1]), Convert.ToInt32(a_date[0]));
                            s_date_check = string.Format("{0}-{1:00}-{2:00}", date_check.Year, date_check.Month, date_check.Day);
                        }
                        if (s_date_check != input_value)
                        {
                            Thread.Sleep(1000);
                            sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                            note_ = string.Format("Lỗi tại bước {0}, không tìm thấy đúng với dữ liệu ngày {2} {1}", order_by, s_value.Replace("'", "''"), input_value);
                            sql_execute += string.Format(" note = '{0}'", note_);
                            sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                            common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                            return false;
                        }
                    }
                }
                Thread.Sleep(Convert.ToInt32(end_time_sleep));
            }
            else if (type_by == "css")
            {
                curElement = driverGC.FindElement(By.Id(s_value));
                bool cont = true;
                int i_loop = 0;
                int n_max = Convert.ToInt32(Convert.ToInt32(in_time_sleep) / 1000);
                while (cont && i_loop < n_max)
                {
                    string css_value = curElement.GetCssValue("display");
                    if (css_value == "none")
                    {
                        cont = false;
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                    i_loop = i_loop + 1;
                }
                if (cont)
                {
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format("Lỗi tại bước {0}, do không tìm thấy id {1} có value display None", order_by, s_value.Replace("'", "''"));
                    sql_execute += string.Format(" note = '{0}'", note_);
                    sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                    Thread.Sleep(1000);
                    return false;
                }
            }
            return bResult;
        }
        public static string ParseCustomDate(string input)
        {
            if (input.Length < 5)
                throw new ArgumentException("Định dạng không hợp lệ");

            string yearPart = input.Substring(0, 4);
            string dayOfYearPart = input.Substring(4);

            int year = int.Parse(yearPart);
            int dayOfYear = int.Parse(dayOfYearPart);

            DateTime result = new DateTime(year, 1, 1).AddDays(dayOfYear - 1);
            return result.ToString("yyyy-MM-dd");
        }
        static int search_ngay_gan_nhat_chua_chay(string server, int port, string database_name, int i_so_ngay_back_date, int only_run_dv_import_config_header, int i_only_run_dv_import_config_auto_header)
        {
            int iResult = i_so_ngay_back_date;
            string[] a_month = new string[12] { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
            int i_only_run_dv_import_config_header = 0;
            try
            {
                i_only_run_dv_import_config_header = Convert.ToInt32(only_run_dv_import_config_header);
            }
            catch (Exception e)
            {
                string erorr_note_ = string.Format("Lỗi tại bước {0}", e.Message);
                i_only_run_dv_import_config_header = 0;
                throw;
            }

            DateTime dDate = DateTime.Now;
            dDate = dDate.AddDays(-1 * i_so_ngay_back_date);
            string s_yyyy_mm_dd = string.Format("{0}-{1:00}-{2:00}", dDate.Year, dDate.Month, dDate.Day);
            string s_date_f2 = string.Format("{0} {1}, {2}", a_month[dDate.Month - 1], dDate.Day, dDate.Year);
            string syyyymmdd = string.Format("{0}{1:00}{2:00}", dDate.Year, dDate.Month, dDate.Day);
            DateTime cur_time = DateTime.Now;
            string yyyy_mm_dd_hh_mm_ss_cur = string.Format("{0}-{1:00}-{2:00} {3:0}:{4:0}:{5:0}", cur_time.Year, cur_time.Month, cur_time.Day, cur_time.Hour, cur_time.Minute, cur_time.Second);

            string sql_execute = "";
            sql_execute = string.Format("select id");
            sql_execute += string.Format(" from dv_ods_import_header");
            sql_execute += string.Format(" where datadate = '{0}'", s_yyyy_mm_dd);
            DataTable data_sql = common_sql.GetData(sql_execute, server, port, database_name);
            string ods_import_header_id = "0";
            if (data_sql.Rows.Count > 0)
            {
                ods_import_header_id = data_sql.Rows[0]["id"].ToString().Trim();
            }
            else
            {
                sql_execute = string.Format("insert into dv_ods_import_header (datadate, name, state, create_date, create_uid, ph_missing, ph_done) ");
                sql_execute += string.Format(" values ('{0}', '{1}', 'draft', '{2}', 1, '', '')", s_yyyy_mm_dd, s_yyyy_mm_dd, yyyy_mm_dd_hh_mm_ss_cur);
                ods_import_header_id = common_sql.ExecuteExecuteScalarPostgree(sql_execute, server, port, database_name).ToString();

            }
            /// thực hiện tìm báo cáo chưa chạy: dv_import_config_auto_header
            sql_execute = "select a.id, a.name ";
            sql_execute += string.Format(" from dv_import_config_auto_header a");
            sql_execute += string.Format(" left join (select * from dv_ods_import_auto_get where ods_import_header_id = {0}) b on a.id = b.import_config_auto_header_id", ods_import_header_id);
            sql_execute += string.Format(" where a.id not in ");
            sql_execute += string.Format(" ( select b.import_config_auto_header_id ");
            sql_execute += string.Format(" from dv_ods_import_auto_get b ");
            sql_execute += string.Format(" where b.ods_import_header_id = {0}", ods_import_header_id);
            sql_execute += string.Format(" and (b.state = 'done' or b.state = 'confirm') and b.import_config_auto_header_id is not null)");
            sql_execute += string.Format(" and a.hieuluc = true");
            if (i_only_run_dv_import_config_auto_header != 0)
            {
                sql_execute += string.Format(" and a.id = {0}", i_only_run_dv_import_config_auto_header);
            }
            else if (i_only_run_dv_import_config_header != 0)
            {
                sql_execute += string.Format(" and a.import_config_header_id = {0}", only_run_dv_import_config_header);
            }
            sql_execute += string.Format(" order by b.so_lan_get NULLS first, a.order_by");
            data_sql = common_sql.GetData(sql_execute, server, port, database_name);
            string import_config_auto_header_id = "0";
            string ods_import_auto_get_id = "0";
            string file_name_download = "";
            if (data_sql.Rows.Count > 0)
            {
                /// Trường hợp này tìm được báo cáo chưa chạy thì bắt đầu tìm xem thử đã được tạo trong status chạy hiện hành của ngày chưa?
                import_config_auto_header_id = data_sql.Rows[0]["id"].ToString().Trim();
                file_name_download = data_sql.Rows[0]["name"].ToString().Trim();
                sql_execute = "";
                sql_execute = string.Format("select b.state, b.id");
                sql_execute += string.Format(" from dv_ods_import_auto_get b");
                sql_execute += string.Format(" where b.ods_import_header_id = {0} and import_config_auto_header_id = {1}", ods_import_header_id, import_config_auto_header_id);
                data_sql = common_sql.GetData(sql_execute, server, port, database_name);
                ods_import_auto_get_id = "0";
                if (data_sql.Rows.Count == 0)
                {
                    /// Trường hợp chưa tạo dòng nào trên header thì tiến hành tạo 1 dòng ở trạng thái draft
                    sql_execute = string.Format("insert into dv_ods_import_auto_get (datadate, name, state, ods_import_header_id, import_config_auto_header_id, create_date, create_uid, so_lan_get) ");
                    sql_execute += string.Format(" values ('{0}', '{1}', 'draft', {3}, {4}, '{2}', 1, 1)", s_yyyy_mm_dd, s_yyyy_mm_dd, yyyy_mm_dd_hh_mm_ss_cur, ods_import_header_id, import_config_auto_header_id);
                    ods_import_auto_get_id = common_sql.ExecuteExecuteScalarPostgree(sql_execute, server, port, database_name).ToString();
                }
                else
                {
                    // chổ này không thực hiện tăng lên mà vẫn lấy như cũ để ra ngoài vòng lặp tạo để tăng số lần lên
                    //ods_import_auto_get_id = data_sql.Rows[0]["id"].ToString().Trim();
                    //sql_execute = string.Format("update dv_ods_import_auto_get set so_lan_get = so_lan_get + 1");
                    //sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                    //common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                }

            }
            else
            {
                /// Trường hợp này là đã lấy hết báo cáo tiến hành không thực hiện chạy gì nữa cả thoát chương trình luôn
                return 0;
            }
            return iResult;
        }

        static void WaitForAllDownloadsComplete(string downloadDir, int timeoutSeconds = 300)
        {
            int waited = 0;
            while (waited < timeoutSeconds)
            {
                var partFiles = Directory.GetFiles(downloadDir, "*.part");
                if (partFiles.Length == 0) // không còn file tạm
                {
                    return;
                }
                Thread.Sleep(1000);
                waited++;
            }
            throw new Exception("Vẫn còn file .part sau thời gian chờ");
        }

        public static IWebDriver? CreateDriver(string downloadPath, string is_delete, string import_config_auto_header_id, string che_do_chay_nhe_nhat, string tempProfile)
        {
            Logger.LogInfo($"Begin 1");
            bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            string basePath = Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory,
                "chrome_bin",
                isWindows ? "windows" : "linux"
            );
            string driverFile = isWindows ? "chromedriver.exe" : "chromedriver";
            string chromeFile = isWindows ? "chrome.exe" : "chrome";
            Logger.LogInfo($"Begin basePath {basePath}");

            string driverPath = Path.Combine(basePath, driverFile);
            string chromePath = Path.Combine(basePath, chromeFile);
            Logger.LogInfo($"Begin 3 driverPath {driverPath}");
            if (!File.Exists(driverPath))
            {
                throw new FileNotFoundException($"Không tìm thấy ChromeDriver tại: {driverPath}");
            }

            if (!File.Exists(chromePath))
            {
                throw new FileNotFoundException($"Không tìm thấy Chrome binary tại: {chromePath}");
            }

            Logger.LogInfo($"Begin Khởi tạo driver CreateDefaultService tại {basePath} : {driverFile}");
            var service = ChromeDriverService.CreateDefaultService(basePath, driverFile);
            service.HideCommandPromptWindow = true;
            Logger.LogInfo($"End Khởi tạo driver CreateDefaultService tại {basePath} : {driverFile}");

            var options = new ChromeOptions();
            options.BinaryLocation = chromePath;
            options.AddArgument("--disable-features=Upgrade-Insecure-Requests,HTTPS-First-Mode");
            options.AddArgument("--disable-features=Strict-OriginIsolation,SchemefulSameSite");
            options.AddArgument("--user-data-dir=" + tempProfile);
            options.AddArgument("--test-type");
            options.AddArgument("--allow-running-insecure-content");
            options.AddArgument("--ignore-certificate-errors");

            //options.AddArgument(@"--user-data-dir=C:\Users\bidvadmin\AppData\Local\Google\Chrome\User Data");
            //options.AddArgument("--profile-directory=Default");
            //options.AddArgument("--ignore-certificate-errors");
            //options.AddArgument("--allow-insecure-localhost");

            // ⚙️ Cấu hình chế độ tải file
            if (is_delete != "true")
            {
                downloadPath = Path.Combine(downloadPath, @$"ID_AUTO_{import_config_auto_header_id}");
                if (!Directory.Exists(downloadPath))
                {
                    Directory.CreateDirectory(downloadPath);
                }
            };
            options.AddUserProfilePreference("download.default_directory", downloadPath);
            options.AddUserProfilePreference("download.prompt_for_download", false);
            //options.AddUserProfilePreference("safebrowsing.enabled", true);
            options.AddUserProfilePreference("safebrowsing.disable_download_protection", true);
            options.AddUserProfilePreference("profile.default_content_settings.popups", 0);
            options.AddUserProfilePreference("profile.content_settings.pattern_pairs.*.multiple-automatic-downloads", 1);

            if (che_do_chay_nhe_nhat == "1")
            {
                // ⚙️ Bật chế độ chạy nhẹ nhất
                options.AddArgument("--headless=new");                    // ẩn giao diện (bắt buộc new để ổn định từ Chrome 112+)
                options.AddArgument("--single-process");                  // chỉ dùng 1 process
                options.AddArgument("--disable-gpu");                     // tắt GPU (không cần render)
                options.AddArgument("--no-sandbox");                      // bỏ sandbox (giảm tiêu tốn tài nguyên)
                options.AddArgument("--disable-dev-shm-usage");           // tránh lỗi memory khi chạy headless
                options.AddArgument("--disable-software-rasterizer");     // tắt rasterizer GPU
                options.AddArgument("--disable-extensions");              // tắt toàn bộ extension
                options.AddArgument("--disable-notifications");           // tắt popup notification
                options.AddArgument("--window-size=1920,1080");           // mô phỏng độ phân giải chuẩn
                options.AddArgument("--mute-audio");                      // tắt âm thanh (nếu có)
                options.AddArgument("--no-default-browser-check");
                options.AddArgument("--disable-popup-blocking");
            }    
            // ⚙️ Tùy chọn giảm load hệ thống
            options.AddArgument("--disable-background-networking");
            options.AddArgument("--disable-background-timer-throttling");
            options.AddArgument("--disable-client-side-phishing-detection");
            options.AddArgument("--disable-default-apps");
            options.AddArgument("--disable-hang-monitor");
            options.AddArgument("--disable-prompt-on-repost");
            options.AddArgument("--disable-sync");
            options.AddArgument("--metrics-recording-only");
            options.AddArgument("--no-first-run");
            options.AddArgument("--disable-translate");

            //options.BinaryLocation = chromePath;
            //options.AddUserProfilePreference("download.prompt_for_download", false);
            //options.AddUserProfilePreference("safebrowsing.disable_download_protection", true);
            //if (is_delete != "true")
            //{
            //    downloadPath = Path.Combine(downloadPath, @$"ID_AUTO_{import_config_auto_header_id}");
            //    if (!Directory.Exists(downloadPath))
            //    {
            //        Directory.CreateDirectory(downloadPath);
            //    }
            //}    
            //options.AddUserProfilePreference("download.default_directory", downloadPath);

            //options.AddUserProfilePreference("download.prompt_for_download", false);
            //options.AddUserProfilePreference("download.directory_upgrade", true);
            //options.AddUserProfilePreference("safebrowsing.enabled", true);
            //options.AddUserProfilePreference("safebrowsing.disable_download_protection", true);
            //options.AddUserProfilePreference("profile.default_content_settings.popups", 0);
            //options.AddUserProfilePreference("profile.content_settings.pattern_pairs.*.multiple-automatic-downloads", 1);

            // Tùy chọn bỏ banner “Keep file”
            //options.AddArgument("--safebrowsing-disable-download-protection");
            //options.AddArgument("--no-first-run");
            //options.AddArgument("--no-default-browser-check");
            //options.AddArgument("--disable-popup-blocking");
            //options.AddArgument("--disable-notifications");
            //options.AddArgument("--allow-running-insecure-content");
            //options.AddArgument("--allow-insecure-localhost");
            //options.AddArgument("--ignore-certificate-errors");
            //options.AddArgument("--safebrowsing-disable-download-protection");
            //options.AddArgument("--disable-features=BlockInsecurePrivateNetworkRequests,InsecureDownloadWarnings,SafeBrowsingEnhancedProtection");

            TimeSpan initTimeout = TimeSpan.FromMinutes(3);

            try
            {
                var driver = new ChromeDriver(service, options, initTimeout); // REFACTORED
                var devTools = driver.GetDevToolsSession();

                // ✅ CDP API đồng bộ
                //driver.ExecuteCdpCommand("Page.enable", new Dictionary<string, object>());

                driver.ExecuteCdpCommand("Page.setDownloadBehavior", new Dictionary<string, object>
                {
                    ["behavior"] = "allow",
                    ["downloadPath"] = downloadPath
                });
                var parameters = new Dictionary<string, object>() { { "ignore", true } };
                driver.ExecuteCdpCommand("Security.setIgnoreCertificateErrors", parameters);
                return driver;
            }
            catch (WebDriverException ex)
            {
                string error = $"[ERROR] Lỗi khởi tạo ChromeDriver. Có thể Chrome và ChromeDriver không cùng version. Chi tiết: {ex.Message}";
                throw;
            }
            catch (Exception ex)
            {
                string error = $"[ERROR] Lỗi không xác định khi khởi tạo ChromeDriver: {ex.Message}";
                throw;
            }
        }

        public static IWebDriver GetWebDriver(string is_chrome_browser, string downloadPath, string version, string version_firerfox, string online_version, 
            string is_delete, string import_config_auto_header_id, string che_do_chay_nhe_nhat, string tempProfile)
        {
            IWebDriver driver;

            Logger.LogInfo($"Begin GetWebDriver is_chrome_browser {is_chrome_browser}");
            if (is_chrome_browser == "1")
            {
                Logger.LogInfo($"Begin CreateDriver Chrome");
                driver = CreateDriver(downloadPath, is_delete, import_config_auto_header_id, che_do_chay_nhe_nhat, tempProfile);
                //ChromeOptions chromOption = new ChromeOptions();
                //chromOption.AddUserProfilePreference("download.prompt_for_download", false);
                //chromOption.AddUserProfilePreference("safebrowsing.disable_download_protection", true);
                //chromOption.AddUserProfilePreference("download.default_directory", downloadPath);
                //if (online_version == "1")
                //{
                //    new DriverManager().SetUpDriver(new ChromeConfig());
                //    driver = new ChromeDriver(chromOption);
                //}
                //else
                //{
                //    string driverPath = $@"C:\chromedriver\{version}\driver\win32";
                //    driver = new ChromeDriver(driverPath, chromOption);
                //}
               //driver = new ChromeDriver(chromOption);
            }
            else // Firefox
            {
                Logger.LogInfo($"Begin CreateDriver Firefox");
                FirefoxOptions ffOptions = new FirefoxOptions();
                FirefoxProfile profile = new FirefoxProfile();
                profile.SetPreference("browser.download.folderList", 2);
                profile.SetPreference("browser.download.dir", downloadPath);
                profile.SetPreference("browser.helperApps.neverAsk.saveToDisk",
                    "application/pdf,application/zip,text/csv,application/octet-stream");
                profile.SetPreference("pdfjs.disabled", true);
                ffOptions.Profile = profile;
                if (online_version == "1")
                {
                    new DriverManager().SetUpDriver(new FirefoxConfig());
                    driver = new FirefoxDriver(ffOptions);
                }
                else
                {
                    string driverPath = $@"D:\geckodriver\{version_firerfox}\";
                    driver = new FirefoxDriver(driverPath, ffOptions);
                }
                //driver = new FirefoxDriver(ffOptions);
            }

            //driver.Manage().Window.Maximize();
            return driver;
        }
        static bool TryMoveDownloadedFile(string downloadRoot, string file_name_download, 
            string is_delete, string import_config_auto_header_id, int timeoutSeconds = 120)
        {
            try
            {
                // 🔹 Thư mục chứa file tải
                string subFolder = downloadRoot;
                if (is_delete != "true")
                {
                    subFolder = Path.Combine(downloadRoot, @$"ID_AUTO_{import_config_auto_header_id}");
                }    
                if (!Directory.Exists(subFolder))
                {
                    Console.WriteLine($"[WARN] Không tồn tại thư mục: {subFolder}");
                    return false;
                }

                string targetFilePath = Path.Combine(subFolder, file_name_download);
                string crDownload = targetFilePath + ".crdownload"; // file tạm của Chrome

                int waited = 0;
                while (waited < timeoutSeconds)
                {
                    if (File.Exists(targetFilePath) && !File.Exists(crDownload))
                        break;

                    Thread.Sleep(1000);
                    waited++;
                }

                // 🔹 Hết thời gian chờ mà file vẫn chưa sẵn sàng
                if (!File.Exists(targetFilePath) || File.Exists(crDownload))
                {
                    Console.WriteLine($"[ERROR] File chưa sẵn sàng sau {timeoutSeconds}s: {targetFilePath}");
                    return false;
                }

                // 🔹 Chuẩn bị tên file mới khi copy ra thư mục gốc
                if (is_delete != "true")
                {
                    string ext = Path.GetExtension(targetFilePath);
                    string fileNameWithoutExt = Path.GetFileNameWithoutExtension(targetFilePath);

                    string destFileName = $"{fileNameWithoutExt}_id_auto_{import_config_auto_header_id}{ext}";
                    string destFilePath = Path.Combine(downloadRoot, destFileName);

                    destFilePath = GetNewFile(destFilePath); // tránh trùng tên

                    // 🔹 Thực hiện copy
                    File.Copy(targetFilePath, destFilePath, overwrite: false);
                    Console.WriteLine($"[OK] File tải và copy thành công: {destFileName}");
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Lỗi khi copy file: {ex.Message}");
                throw;
            }
        }

        static IWebElement? FindElementWithAutoFrame(IWebDriver driver, By by, int timeoutSeconds = 10)
        {
            WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromSeconds(timeoutSeconds));

            // Thử tìm trong main DOM
            try
            {
                return wait.Until(ExpectedConditions.ElementExists(by));
            }
            catch { }

            // Lấy danh sách tất cả các iframe
            var frames = driver.FindElements(By.TagName("iframe"));
            for (int i = 0; i < frames.Count; i++)
            {
                try
                {
                    driver.SwitchTo().DefaultContent();
                    driver.SwitchTo().Frame(i);

                    var element = wait.Until(ExpectedConditions.ElementExists(by));
                    Console.WriteLine($"[INFO] Tìm thấy element trong iframe index {i}");
                    return element;
                }
                catch { }
            }

            // Không tìm thấy → quay về main DOM
            driver.SwitchTo().DefaultContent();
            return null;
        }

        static void Main(string[] args)
        {
            Logger.LogInfo($"Khởi tạo tại {DateTime.Now}");
            IWebDriver driverGC = null;
            string import_config_auto_header_id = "0";
            string ods_import_auto_get_id = "0";
            string file_name_download = "";
            string note_auto_header = "";
            DataTable data_sql;
            string full_file_name_dl = "";
            string s_yyyy_mm_dd = "";
            string s_date_f2 = "";
            string s_date_f3 = "";
            string syyyymmdd = "";
            string yyyy_mm_dd_hh_mm_ss_cur = "";
            string is_data = "";
            string FolderDownloadCur = "";
            string is_delete = "true";
            string server = ConfigurationManager.AppSettings["server"] ?? "10.130.2.20";
            int port = Convert.ToInt32(ConfigurationManager.AppSettings["port"] ?? "5432");
            string database_name = ConfigurationManager.AppSettings["database_name"] ?? "bidv_hcm_001";
            string path_download = ConfigurationManager.AppSettings["path_download"] ?? "D:\\AutoGetODS";
            string so_ngay_back_date = ConfigurationManager.AppSettings["so_ngay_back_date"] ?? "1";
            string only_run_dv_import_config_header = ConfigurationManager.AppSettings["only_run_dv_import_config_header"] ?? "0";
            string only_run_dv_import_config_auto_header = ConfigurationManager.AppSettings["only_run_dv_import_config_auto_header"] ?? "0";
            var version = ConfigurationManager.AppSettings["version"] ?? "103.0.5060.134";
            var version_firerfox = ConfigurationManager.AppSettings["version_firerfox"] ?? "103.0.5060.134";
            string quit_browse = ConfigurationManager.AppSettings["quit_browse"] ?? "0";
            string online_version = ConfigurationManager.AppSettings["online_version"] ?? "0";
            string is_browse_chrome = ConfigurationManager.AppSettings["is_browse_chrome"] ?? "0" ;
            string che_do_chay_nhe_nhat = ConfigurationManager.AppSettings["che_do_chay_nhe_nhat"] ?? "0";
            string connection_type = (ConfigurationManager.AppSettings["connection_type"] ?? "sql").Trim().ToLower();

            if (connection_type == "json")
            {
                Logger.LogInfo("Chạy theo JSON mode (connection_type=json)");
                JsonModeRunner.RunFromConfig();
                return;
            }

            string[] a_month = new string[12] { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
            DateTime dDate = DateTime.Now;
            int i_only_run_dv_import_config_header = 0;
            int i_only_run_dv_import_config_auto_header = int.TryParse(only_run_dv_import_config_auto_header, out int result) ? result : 0;
            string sql_execute = "";
            string note_ = "";
            string order_by = "";
            string erorr_note_ = "";
            Logger.LogInfo($"Bat dau try {DateTime.Now}");
            string tempProfile = Path.Combine("c:\\temp\\sel_profile_" + Guid.NewGuid());
            try
            {

                try
                {
                    i_only_run_dv_import_config_header = Convert.ToInt32(only_run_dv_import_config_header);
                    Logger.LogInfo($"Bat dau try 1");
                }
                catch (Exception e)
                {
                    erorr_note_ = string.Format("Lỗi tại bước {0}", e.Message);
                    i_only_run_dv_import_config_header = 0;
                    throw;
                }
                int i_so_ngay_back_date = 1;
                try
                {
                    i_so_ngay_back_date = Convert.ToInt32(so_ngay_back_date);
                    Logger.LogInfo($"Bat dau try i_only_run_dv_import_config_header: {i_only_run_dv_import_config_header}");
                    if (i_only_run_dv_import_config_header > 0 || i_only_run_dv_import_config_auto_header > 0)
                    {
                        bool cont = true;
                        Logger.LogInfo($"Bat dau try 2");
                        while (cont && i_so_ngay_back_date >= 1)
                        {
                            int ods_import_auto_get_id_temp = search_ngay_gan_nhat_chua_chay(server, port, database_name, i_so_ngay_back_date, i_only_run_dv_import_config_header, i_only_run_dv_import_config_auto_header);
                            if (ods_import_auto_get_id_temp > 0)
                            {
                                cont = false;
                            }
                            else
                            {
                                i_so_ngay_back_date = i_so_ngay_back_date - 1;
                            }
                        }
                        Logger.LogInfo($"end try 2.1");
                    }
                }
                catch (Exception e)
                {
                    erorr_note_ = string.Format("Lỗi tại bước {0}", e.Message);
                    Logger.LogInfo(erorr_note_);
                    i_so_ngay_back_date = 1;
                    throw;
                }
                if (i_so_ngay_back_date == 0)
                {
                    erorr_note_ = "Giá trị i_so_ngay_back_date = 0 — logic không hợp lệ.";
                    Logger.LogInfo(erorr_note_);
                    throw new Exception(erorr_note_);
                }
                try
                {
                    dDate = dDate.AddDays(-1 * i_so_ngay_back_date);
                    s_yyyy_mm_dd = string.Format("{0}-{1:00}-{2:00}", dDate.Year, dDate.Month, dDate.Day);
                    s_date_f2 = string.Format("{0} {1}, {2}", a_month[dDate.Month - 1], dDate.Day, dDate.Year);
                    s_date_f3 = $"{dDate.Year}{dDate.DayOfYear}"; ;
                    syyyymmdd = string.Format("{0}{1:00}{2:00}", dDate.Year, dDate.Month, dDate.Day);
                    DateTime cur_time = DateTime.Now;
                    yyyy_mm_dd_hh_mm_ss_cur = string.Format("{0}-{1:00}-{2:00} {3:0}:{4:0}:{5:0}", cur_time.Year, cur_time.Month, cur_time.Day, cur_time.Hour, cur_time.Minute, cur_time.Second);

                    sql_execute = "";
                    sql_execute = string.Format("select id");
                    sql_execute += string.Format(" from dv_ods_import_header");
                    sql_execute += string.Format(" where datadate = '{0}'", s_yyyy_mm_dd);
                    data_sql = common_sql.GetData(sql_execute, server, port, database_name);
                    string ods_import_header_id = "0";
                    if (data_sql.Rows.Count > 0)
                    {
                        ods_import_header_id = data_sql.Rows[0]["id"].ToString().Trim();
                    }
                    else
                    {
                        sql_execute = string.Format("insert into dv_ods_import_header (datadate, name, state, create_date, create_uid, ph_missing, ph_done) ");
                        sql_execute += string.Format(" values ('{0}', '{1}', 'draft', '{2}', 1, '', '')", s_yyyy_mm_dd, s_yyyy_mm_dd, yyyy_mm_dd_hh_mm_ss_cur);
                        ods_import_header_id = common_sql.ExecuteExecuteScalarPostgree(sql_execute, server, port, database_name).ToString();

                    }
                    /// thực hiện tìm báo cáo chưa chạy: dv_import_config_auto_header
                    sql_execute = "select a.id, a.name, a.note ";
                    sql_execute += string.Format(" from dv_import_config_auto_header a");
                    sql_execute += string.Format(" left join (select * from dv_ods_import_auto_get where ods_import_header_id = {0}) b on a.id = b.import_config_auto_header_id", ods_import_header_id);
                    sql_execute += string.Format(" where a.id not in ");
                    sql_execute += string.Format(" ( select b.import_config_auto_header_id ");
                    sql_execute += string.Format(" from dv_ods_import_auto_get b ");
                    sql_execute += string.Format(" where b.ods_import_header_id = {0}", ods_import_header_id);
                    sql_execute += string.Format(" and (b.state = 'done' or b.state = 'confirm') and b.import_config_auto_header_id is not null)");
                    sql_execute += string.Format(" and a.hieuluc = true");
                    if (i_only_run_dv_import_config_auto_header != 0)
                    {
                        sql_execute += string.Format(" and a.id = {0}", i_only_run_dv_import_config_auto_header);
                    }
                    else if (i_only_run_dv_import_config_header != 0)
                    {
                        sql_execute += string.Format(" and a.import_config_header_id = {0}", only_run_dv_import_config_header);
                    }
                    sql_execute += string.Format(" order by b.so_lan_get NULLS first, a.order_by");
                    data_sql = common_sql.GetData(sql_execute, server, port, database_name);
                    if (data_sql.Rows.Count > 0)
                    {
                        /// Trường hợp này tìm được báo cáo chưa chạy thì bắt đầu tìm xem thử đã được tạo trong status chạy hiện hành của ngày chưa?
                        import_config_auto_header_id = data_sql.Rows[0]["id"].ToString().Trim();
                        file_name_download = data_sql.Rows[0]["name"].ToString().Trim();
                        if (data_sql.Rows[0]["note"].ToString().Trim() != "")
                        {
                            note_auto_header = data_sql.Rows[0]["note"].ToString().Trim();
                        }                         
                        sql_execute = "";
                        sql_execute = string.Format("select b.state, b.id");
                        sql_execute += string.Format(" from dv_ods_import_auto_get b");
                        sql_execute += string.Format(" where b.ods_import_header_id = {0} and import_config_auto_header_id = {1}", ods_import_header_id, import_config_auto_header_id);
                        data_sql = common_sql.GetData(sql_execute, server, port, database_name);
                        ods_import_auto_get_id = "0";
                        if (data_sql.Rows.Count == 0)
                        {
                            /// Trường hợp chưa tạo dòng nào trên header thì tiến hành tạo 1 dòng ở trạng thái draft
                            sql_execute = string.Format("insert into dv_ods_import_auto_get (datadate, name, state, ods_import_header_id, import_config_auto_header_id, create_date, create_uid, so_lan_get) ");
                            sql_execute += string.Format(" values ('{0}', '{1}', 'draft', {3}, {4}, '{2}', 1, 1)", s_yyyy_mm_dd, s_yyyy_mm_dd, yyyy_mm_dd_hh_mm_ss_cur, ods_import_header_id, import_config_auto_header_id);
                            ods_import_auto_get_id = common_sql.ExecuteExecuteScalarPostgree(sql_execute, server, port, database_name).ToString();
                        }
                        else
                        {
                            ods_import_auto_get_id = data_sql.Rows[0]["id"].ToString().Trim();
                            sql_execute = string.Format("update dv_ods_import_auto_get set so_lan_get = so_lan_get + 1");
                            sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                            common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                        }

                    }
                    else
                    {
                        /// Trường hợp này là đã lấy hết báo cáo tiến hành không thực hiện chạy gì nữa cả thoát chương trình luôn
                        return;
                    }
                    /// Lấy thông tin script chạy tiếp theo cho việc get dữ liệu
                    sql_execute = "";
                    sql_execute = string.Format("select a.type_by, case when a.import_config_master_script_id is null then a.s_value else c.s_value end s_value, a.input_value, a.begin_time_sleep, a.end_time_sleep, b.stt_lv_1, b.stt_lv_2, a.order_by");
                    sql_execute += string.Format(" , a.is_click, a.is_click_ac, a.in_time_sleep, b.stt_lv_3, a.is_popup_download");
                    sql_execute += string.Format(" , b.folder_sub, b.is_delete, b.is_data, b.sql_finish");
                    sql_execute += string.Format(" from dv_import_config_auto_detail a");
                    sql_execute += string.Format(" left join dv_import_config_auto_header b on a.import_config_auto_header_id = b.id");
                    sql_execute += string.Format(" left join dv_import_config_master_script c on a.import_config_master_script_id = c.id");
                    sql_execute += string.Format(" where a.import_config_auto_header_id = {0} and a.hieuluc = true", import_config_auto_header_id);
                    sql_execute += string.Format(" order by a.order_by");
                    data_sql = common_sql.GetData(sql_execute, server, port, database_name);
                    note_ = "";
                    if (data_sql.Rows.Count == 0)
                    {
                        sql_execute = string.Format("update dv_ods_import_auto_get set state = 'confirm', ");
                        note_ = string.Format($"Không tồn tại bất kỳ script nào. xin vui lòng kiểm tra chi tiết script, Ghi chú {note_auto_header}");
                        sql_execute += string.Format(" note = '{0}'", note_);
                        sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                        common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                        return;
                    }
                    string folder_sub = data_sql.Rows[0]["folder_sub"].ToString().Trim();
                    is_delete = data_sql.Rows[0]["is_delete"].ToString().Trim().ToLower();
                    is_data = data_sql.Rows[0]["is_data"].ToString().Trim().ToLower();
                    

                    //Bắt đầu mở trình duyệt
                    FolderDownloadCur = string.Format(@"{0}\{1}", path_download, syyyymmdd);
                    if (!System.IO.Directory.Exists(FolderDownloadCur))
                    {
                        System.IO.Directory.CreateDirectory(FolderDownloadCur);
                    }
                    /// Trường hợp có thư mục con để dưa vào
                    if (folder_sub != "")
                    {
                        FolderDownloadCur = string.Format(@"{0}\{1}", FolderDownloadCur, folder_sub);
                        if (!System.IO.Directory.Exists(FolderDownloadCur))
                        {
                            System.IO.Directory.CreateDirectory(FolderDownloadCur);
                        }
                    }
 
                    ///Thực hiện xóa file hiện hành nếu tồn tại file
                    full_file_name_dl = string.Format(@"{0}\{1}", FolderDownloadCur, file_name_download);
                    if (is_delete == "true")
                    {
                        if (File.Exists(full_file_name_dl))
                        {
                            File.Delete(full_file_name_dl);
                        }
                    }
                    else
                    {
                        full_file_name_dl = GetNewFile(full_file_name_dl);
                    }
                }
                catch (Exception e)
                {
                    Thread.Sleep(1000);
                    erorr_note_ = string.Format("Lỗi: {0}", e.Message);
                    i_so_ngay_back_date = 1;
                    throw;
                }
                /// BEGIN MỞ TRÌNH DUYỆT VÀ CHẠY SCRIPT
                /// 
                Logger.LogInfo($"Khởi tạo ChromeDriver tại {DateTime.Now}");
                driverGC = GetWebDriver(is_browse_chrome, FolderDownloadCur, version, version_firerfox, online_version, is_delete, import_config_auto_header_id, che_do_chay_nhe_nhat, tempProfile);
                driverGC.Manage().Window.Maximize();
                Actions actions = new Actions(driverGC);
                sql_execute = string.Format("update dv_ods_import_auto_get set state = 'confirm', note_lastest = note, ");
                note_ = string.Format($"Đang lấy dữ liệu ..., Ghi chú {note_auto_header}");
                sql_execute += string.Format(" note = '{0}'", note_);
                sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();

                string s_result_data = "";
                int i = 0;
                string s_value = "";
                try
                {
                    IJavaScriptExecutor js = (IJavaScriptExecutor)driverGC;  /// Khởi tạo một java script click
                    if (data_sql.Rows.Count > 0)
                    {
                        /// Cập nhật sang trạng thái đang lấy dữ liệu

                        while (i < data_sql.Rows.Count)
                        {
                            string stt_lv_1 = data_sql.Rows[i]["stt_lv_1"].ToString().Trim();
                            string stt_lv_2 = data_sql.Rows[i]["stt_lv_2"].ToString().Trim();
                            string stt_lv_3 = data_sql.Rows[i]["stt_lv_3"].ToString().Trim();
                            s_value = data_sql.Rows[i]["s_value"].ToString().Trim();
                            string input_value = data_sql.Rows[i]["input_value"].ToString().Trim();
                            input_value = input_value.Replace("para_datadate", s_yyyy_mm_dd);
                            input_value = input_value.Replace("para_date_f2", s_date_f2);
                            input_value = input_value.Replace("para_date_f3", s_date_f3);
                            order_by = data_sql.Rows[i]["order_by"].ToString().Trim();
                            string is_click = data_sql.Rows[i]["is_click"].ToString().Trim().ToLower();
                            string is_click_ac = data_sql.Rows[i]["is_click_ac"].ToString().Trim().ToLower();
                            string is_popup_download = data_sql.Rows[i]["is_popup_download"].ToString().Trim().ToLower();
                            string in_time_sleep = data_sql.Rows[i]["in_time_sleep"].ToString().Trim().ToLower();
                            s_value = s_value.Replace("para_lv1", stt_lv_1);
                            s_value = s_value.Replace("para_lv2", stt_lv_2);
                            s_value = s_value.Replace("para_lv3", stt_lv_3);
                            string begin_time_sleep = data_sql.Rows[i]["begin_time_sleep"].ToString().Trim();
                            string end_time_sleep = data_sql.Rows[i]["end_time_sleep"].ToString().Trim();
                            string type_by = data_sql.Rows[i]["type_by"].ToString().Trim();
                            string sql_finish = data_sql.Rows[i]["sql_finish"].ToString().Trim();
                            if (i == 8)
                            {
                                string aa = "bắt đầu debug chổ này";
                            }
                            if (order_by == "10")
                            {
                                int debug_h = 1;
                            }    
                            bool bresult = ActrionOneStep(ref driverGC, ref actions, ref i, ref s_result_data,
                                type_by, begin_time_sleep, in_time_sleep, end_time_sleep,
                                s_value, order_by, ods_import_auto_get_id,
                                server, port, database_name,
                                is_click, input_value, is_click_ac, is_data, sql_finish, is_popup_download, FolderDownloadCur);
                            if (bresult == false)
                            {
                                if (quit_browse == "1")
                                {
                                    driverGC.Close();
                                    driverGC.Quit();
                                    driverGC.Dispose();
                                    driverGC = null;
                                }
                                return;
                            }
                            i = i + 1;
                        }
                    }
                }
                catch (Exception e)
                {
                    throw;
                }
                /// chờ tối đa 2 phút để tải file
                bool fileCopied = TryMoveDownloadedFile(FolderDownloadCur, file_name_download, is_delete, import_config_auto_header_id, 120);

                if (fileCopied)
                {
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'done', ");
                    note_ = string.Format($"Tải file và copy thành công.{s_result_data}, Ghi chú {note_auto_header}");
                }
                else
                {
                    sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                    note_ = string.Format($"Không tìm thấy file sau khi tải, cần thử lại. Ghi chú {note_auto_header}");
                }

                sql_execute += string.Format(" note = '{0}'", note_);
                sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();

                Thread.Sleep(2000);
                if (quit_browse == "1")
                {
                    driverGC.Close();
                    driverGC.Quit();
                    driverGC.Dispose();
                    driverGC = null;
                }
            }
            catch (Exception ex)
            {
                Thread.Sleep(1000);
                string message = ex?.Message ?? "Không xác định";
                string fullError = $"[ERROR] {DateTime.Now:yyyy-MM-dd HH:mm:ss} | Bước: {order_by} | Lỗi: {message}";
                if (!string.IsNullOrEmpty(ods_import_auto_get_id) && ods_import_auto_get_id != "0" && !string.IsNullOrEmpty(server) && !string.IsNullOrEmpty(database_name))
                {
                    string safeMessage = message.Replace("'", "''");
                    sql_execute = $@"
                    update dv_ods_import_auto_get 
                    set state = 'draft', 
                        note = 'Lỗi tại bước {order_by}, Exception: {safeMessage} , Ghi chú: {note_auto_header}'
                    where id = {ods_import_auto_get_id}";
                    common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                }
                //sql_execute = string.Format("update dv_ods_import_auto_get set state = 'draft', ");
                //note_ = string.Format("Lỗi tại bước {0}, Exception: {1}", order_by, e.Message);
                //sql_execute += string.Format(" note = '{0}'", note_);
                //sql_execute += string.Format(" where id = {0}", ods_import_auto_get_id);
                //common_sql.ExecuteNoneQueryPostgree(sql_execute, server, port, database_name).ToString();
                Thread.Sleep(1000);
                if (quit_browse == "1" && driverGC != null)
                {
                    driverGC.Close();
                    driverGC.Quit();
                    driverGC.Dispose();
                    driverGC = null;
                }
                Logger.LogInfo(fullError);

            }
            finally
            {
                try
                {
                    if (driverGC != null)
                    {
                        if (quit_browse == "1")
                        {
                            driverGC.Close();
                            driverGC.Quit();
                            driverGC.Dispose();
                            driverGC = null;
                        }
                    }
                    Directory.Delete(tempProfile, true);
                    // Bảo đảm kill cả tiến trình Chrome & ChromeDriver
                    //KillChromeProcesses();
                }
                catch (Exception e)
                {
                    Logger.LogInfo("[WARN] Không thể đóng Chrome: " + e.Message);
                    Console.WriteLine("[WARN] Không thể đóng Chrome: " + e.Message);
                }
            }

        }

    }
}
