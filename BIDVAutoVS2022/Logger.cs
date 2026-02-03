using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BIDVAutoVS2022
{
    public static class Logger
    {
        private static readonly object _lock = new object();
        private static string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");

        public static void LogInfo(string message)
        {
            WriteLog("INFO", message);
        }

        public static void LogWarning(string message)
        {
            WriteLog("WARN", message);
        }

        public static void LogError(string message, Exception ex = null)
        {
            string errorMsg = message;
            if (ex != null)
            {
                errorMsg += $"\n[EXCEPTION]: {ex.Message}\n[STACKTRACE]: {ex.StackTrace}";
            }
            WriteLog("ERROR", errorMsg);
        }

        private static void WriteLog(string level, string message)
        {
            try
            {
                lock (_lock)
                {
                    if (!Directory.Exists(logDirectory))
                        Directory.CreateDirectory(logDirectory);

                    string logFile = Path.Combine(logDirectory, $"{DateTime.Now:yyyyMMdd}.log");
                    string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{level}] {message}";
                    File.AppendAllText(logFile, line + Environment.NewLine);
                }
            }
            catch
            {
                // Không throw exception nếu log lỗi
            }
        }
    }
}
