using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using System.Data.OleDb;

namespace XML
{
    class MyExcel
    {
        string GetExcelConnectionString(FileInfo _file)
        {
            Dictionary<string, string> props = new Dictionary<string, string>();

            if (_file.Extension == ".xlsx")
            {                                                                   
                props["Provider"] = "Microsoft.ACE.OLEDB.12.0;";
                props["Extended Properties"] = "Excel 12.0 XML";
                props["Data Source"] = _file.FullName;
            }
            else if (_file.Extension == ".xls")
            {
                props["Provider"] = "Microsoft.Jet.OLEDB.4.0";
                props["Extended Properties"] = "Excel 8.0";
                props["Data Source"] = _file.FullName;
            }
            else throw new Exception("Неизвестное расширение файла!");

            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, string> prop in props)
            {
                sb.Append(prop.Key);
                sb.Append('=');
                sb.Append(prop.Value);
                sb.Append(';');
            }
            return sb.ToString();
        }

        private void loadExcelFile(FileInfo _file)
        {
            DataSet ds = new DataSet();                                                                                     //инициализация данных excel-я
            string connectionString = GetExcelConnectionString(_file);                                                      //получение строки для подключения к файлу

            using (OleDbConnection conn = new OleDbConnection(connectionString))
            {                                          //подключение к файлу..

                conn.Open();                                                                                                //открытие канала чтения
                OleDbCommand cmd = new OleDbCommand();                                                                      //инициализация комманды получения данных
                cmd.Connection = conn;                                                                                      //ссылка команде на подключение

                DataTable dtSheet = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);                                 //получение всех страниц файла

                // Loop through all Sheets to get data
                foreach (DataRow dr in dtSheet.Rows)
                {
                    string sheetName = dr["TABLE_NAME"].ToString();

                    // Get all rows from the Sheet                              
                    cmd.CommandText = "SELECT * FROM [" + sheetName + "]";

                    DataTable dt = new DataTable();
                    dt.TableName = sheetName;

                    OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                    da.Fill(dt);

                    ds.Tables.Add(dt);
                }
            }
            for (int j = 0; j < ds.Tables[0].Columns.Count; j++)                                                           //пробежка по всем столбцам, начиная от первого с данными..
                for (int i = 0; i < ds.Tables[0].Rows.Count; i++)                                                          //..пробежка по всем строкам, начиная с первой (нулевая- заголовки)..
                    double value = Convert.ToDouble(ds.Tables[0].Rows[i].ItemArray[j]);                                    //....получение значения точки

            //вот этот value и есть эллемент ячейки. правда какого он у вас будет типа мне не ведомо. 
            //и может быть индексы не с нуля, а с единицы, но не уверен)

        }
    }
}
