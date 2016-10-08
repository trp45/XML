using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Data;

namespace XML
{
    class MyCsvReader
    {
        private string _tbname;
        private string _csvpath;
        private int _batchsize;
        private Dictionary<string, string> _connectSettings;
        private Dictionary<int, string> _csvFieldName;

        public MyCsvReader(string tbname, string csvpath, Dictionary<string, string> ConnectSettings, int batchsize)
        {
            _tbname = tbname;
            _batchsize = batchsize;
            _csvpath = csvpath;
            _connectSettings = ConnectSettings;
            _csvFieldName = GetCsvField();
            //FileStream fileStream = File.OpenRead(_xmlpath);
        }

        private Dictionary<int, string> GetCsvField()
        {
            Dictionary<int, string> field = new Dictionary<int, string>();
            using (StreamReader csvReader = new StreamReader(_csvpath))
            {
                try
                {
                    string csvLine = csvReader.ReadLine();
                    string[] fieldData = csvLine.Split(';');

                    for (int i = 0; i < fieldData.Length; i++)
                    {
                        field.Add(i, fieldData[i].ToString().Trim());
                    }
                }
                catch (Exception ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    csvReader.Close();
                    csvReader.Dispose();
                }
            }

            return field;
        }
        /// <summary>
        /// Чтение CSV-файла и сохранение записей в таблицу БД
        /// </summary>
        /// <returns>
        /// 0 - всё прошло без ошибок
        /// 1 - не удалось сохранить записи в БД
        /// 2 - ещё что-то не удалось
        /// </returns>
        public int Read()
        {
            // сюда он будет читаться
            DataTable csvData = new DataTable();
            int counter = 0;

            MyDB BulkWriter = new MyDB(_connectSettings, _tbname, _batchsize);

            //Удаление всего
            BulkWriter.Truncate(_tbname);

            Dictionary<string, Type> ht = BulkWriter._Mapping;
            foreach (KeyValuePair<string, Type> str in ht)
            {
                csvData.Columns.Add(str.Key.ToString(), Type.GetType(str.Value.ToString()));
            }

            using (StreamReader csvReader = new StreamReader(_csvpath))
            {
                try
                {
                    while (!csvReader.EndOfStream)
                    {
                        string csvLine = csvReader.ReadLine();
                        DataRow Row = csvData.NewRow();
                        string[] fieldData = csvLine.Split(';');

                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (ht[_csvFieldName[i]] == Type.GetType("System.DateTime"))
                            {
                                Row[_csvFieldName[i]] = DateTime.ParseExact(fieldData[i], "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                Row[_csvFieldName[i]] = fieldData[i];
                            }
                        }

                        csvData.Rows.Add(fieldData);
                        csvData.Rows.Add(Row);
                        counter++;


                        if (counter % _batchsize == 0)
                        {
                            //BulkWriter.BulkInsertAll(csvData);
                            csvData.Clear();
                        }
                    }
                    if (counter != 0)
                    {
                        //BulkWriter.BulkInsertAll(csvData);
                        csvData.Rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    csvReader.Close();
                    csvReader.Dispose();
                }

            }
                                
            // всё огонь
            return 0;
        }
    }
}
