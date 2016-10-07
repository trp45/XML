using System;
using System.Collections.Generic;
//using System.Linq;
using System.Text;
using System.Xml;
//using System.Xml.XPath;
using System.Xml.Schema;
using System.Collections;
using System.Data;
using System.ComponentModel;
using System.IO;


namespace XML
{
    //Класс для работы с XSD схемой
    //Парсинг XSD
    class MyXmlReader
    {
        bool isValid = true;
        //long _fSize;
        private string _tbname;
        private string _xmlpath;
        private string _xsdpath;
        private XmlReader _reader;
        private MyStream progressStreamWrapper;
        private int _batchsize;
        private Dictionary<string, string> _connectSettings;
        private double _pos;

        public MyXmlReader(string tbname, string xmlpath, string xsdpath, Dictionary<string, string> ConnectSettings, int batchsize)
        {
            _tbname = tbname;
            _batchsize = batchsize;
            _xmlpath = xmlpath;
            _xsdpath = xsdpath;
            _connectSettings = ConnectSettings;

            FileStream fileStream = File.OpenRead(_xmlpath);
            progressStreamWrapper = new MyStream(fileStream);
            progressStreamWrapper.PositionChanged += (o, ea) => _pos = (double)progressStreamWrapper.Position / progressStreamWrapper.Length * 100;
            _reader = XmlReader.Create(progressStreamWrapper);
        }

        private void ValidationCallback(object sender, ValidationEventArgs args)
        {
            isValid = false;
            if (args.Severity == XmlSeverityType.Warning)
            {
                String errorMessage;
                errorMessage = "WARNING: ";
                errorMessage = String.Concat(errorMessage, args.Message);
                throw new Exception(errorMessage);
            }
            else if (args.Severity == XmlSeverityType.Error)
            {
                String errorMessage;
                errorMessage = "ERROR: ";
                errorMessage = String.Concat(errorMessage, args.Message);
                throw new Exception(errorMessage);
            }

            Console.WriteLine(args.Message);
        }

        //old version
        //public void DoRead(string xml_path, string xsd_path)
        //{
        //    Hashtable ht = ParseXSD(xsd_path);
        //    string nodename = GetNodes2(xsd_path);
        //    using (XmlReader xml = XmlReader.Create(xml_path))
        //    {

        //        while (xml.Read())
        //        {
        //            if (xml.HasAttributes && xml.Name == nodename)
        //            {
        //                StringBuilder field = new StringBuilder("");
        //                StringBuilder param = new StringBuilder("");
        //                StringBuilder value = new StringBuilder("");
        //                while (xml.MoveToNextAttribute())
        //                {
        //                    field.Append(xml.Name);field.Append(", ");
        //                    param.Append("@"+xml.Name); param.Append(", ");
        //                    if (ht[xml.Name].ToString().Contains("int"))
        //                    {
        //                        value.Append(xml.Value);
        //                    }
        //                    else
        //                    {
        //                        value.Append("'" + xml.Value + "'");
        //                    }
        //                    value.Append(", ");
        //                }
        //                field.Remove(field.Length - 2, 2);
        //                param.Remove(param.Length - 2, 2);
        //                value.Remove(value.Length - 2, 2);
        //                //Console.WriteLine("insert into table(" + field + ") values(" + value + ")");

        //            }
        //        }
        //    }
        //}

        //public void DoRead(DateTime dateupload)
        //{
        //    int counter = 0;
        //    string tab_name = _xmlpath.Substring(_xmlpath.LastIndexOf("\\")+4, _xmlpath.LastIndexOf(".") - _xmlpath.LastIndexOf("\\")-4) + "_NEW";
        //    string nodename = GetDetalNodes(_xsdpath);
        //    DataTable dataTable = new DataTable();


        //    MyDB BulkWriter = new MyDB(_connectSettings, tab_name, _batchsize);

        //    //Удаление всего
        //    BulkWriter.Truncate(tab_name);

        //    Dictionary<string, Type> ht = BulkWriter._Mapping;

        //    foreach (KeyValuePair<string,Type> str in ht)
        //    {
        //        dataTable.Columns.Add(str.Key.ToString(), Type.GetType(str.Value.ToString()));
        //    }


        //    using (_reader)
        //    {
        //        while (_reader.Read())
        //        {
        //            if (_reader.HasAttributes && _reader.Name == nodename)
        //            {
        //                DataRow Row = dataTable.NewRow();
        //                while (_reader.MoveToNextAttribute())
        //                {
        //                    if (ht[_reader.Name] == Type.GetType("System.DateTime"))
        //                    {
        //                        Row[_reader.Name] = DateTime.ParseExact(_reader.Value, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture); 
        //                    }
        //                    else
        //                    {
        //                        Row[_reader.Name] = _reader.Value;
        //                    }
        //                }

        //                dataTable.Rows.Add(Row);
        //                counter++;
                        

        //                if (counter % _batchsize == 0)
        //                {
        //                    BulkWriter.BulkInsertAll(dataTable);
        //                    dataTable.Clear();
        //                }
        //            }
        //        }

        //        BulkWriter.BulkInsertAll(dataTable);
        //    }

        //    _reader.Close();

        //    //Синхронизация/обновление основоной таблицы
        //    BulkWriter.Merge(tab_name);
            
        //    //Запись версии
        //    BulkWriter.WriteVersion(dateupload);
        //}

        public void AsyncRead(BackgroundWorker backgroundWorker, DoWorkEventArgs e, DateTime dateupload, int dbUpdateVer, bool freedate)
        { 
            int counter = 0;
            string tab_name = _tbname.Substring(_tbname.LastIndexOf("\\") + 4, _tbname.LastIndexOf(".") - _tbname.LastIndexOf("\\") - 4) + "_NEW";
            string nodename = GetDetalNodes(_xsdpath);
            DataTable dataTable = new DataTable();


            MyDB BulkWriter = new MyDB(_connectSettings, tab_name, _batchsize);

            //Удаление всего
            BulkWriter.Truncate(tab_name);

            Dictionary<string, Type> ht = BulkWriter._Mapping;

            foreach (KeyValuePair<string, Type> str in ht)
            {
                dataTable.Columns.Add(str.Key.ToString(), Type.GetType(str.Value.ToString()));
            }


            using (_reader)
            {
                while (_reader.Read())
                {
                    if (_reader.HasAttributes && _reader.Name == nodename)
                    {
                        DataRow Row = dataTable.NewRow();
                        while (_reader.MoveToNextAttribute())
                        {
                            if (ht[_reader.Name] == Type.GetType("System.DateTime"))
                            {
                                Row[_reader.Name] = DateTime.ParseExact(_reader.Value, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                Row[_reader.Name] = _reader.Value;
                            }
                        }

                        dataTable.Rows.Add(Row);
                        counter++;
                        

                        if (counter % _batchsize == 0)
                        {
                            BulkWriter.BulkInsertAll(dataTable);
                            dataTable.Clear();

                            if (backgroundWorker.CancellationPending)
                            {
                                e.Cancel = true;
                            }
                            else
                            {
                                backgroundWorker.ReportProgress((int)_pos);
                            }
                        }
                    }
                }

                BulkWriter.BulkInsertAll(dataTable);
            }

            _reader.Close();

            //Синхронизация/обновление основоной таблицы
            BulkWriter.Merge(tab_name);

            //Запись версии
            if (freedate) BulkWriter.WriteVersion(dateupload, dbUpdateVer);

            //Вывод финального процента
            if (backgroundWorker.CancellationPending)
            {
                e.Cancel = true;
            }
            else
            {
                //Удалим все из временной табл
                BulkWriter.Truncate(tab_name);
                _reader.Close();
                progressStreamWrapper.Dispose();

                //TODO: Исправить на динамическое получение кода региона
                //BulkWriter.DeleteAnotherRegion(45);
                backgroundWorker.ReportProgress(100);
            }
        }

        /// <summary>
        /// Читает XSD схему по заданному пути и возвращает строку с иерархией элементов, атрибуты которого надо читать
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        protected string GetNodes(string path)
        {
            string RetStr = "/";
            try
            {

                XmlTextReader reader = new XmlTextReader(path);
                XmlSchema myschema = XmlSchema.Read(reader, ValidationCallback);
                XmlSchemaSet schemaSet = new XmlSchemaSet();
                schemaSet.ValidationEventHandler += new ValidationEventHandler(ValidationCallback);
                schemaSet.Add(myschema);
                schemaSet.Compile();

                foreach (XmlSchemaElement element in myschema.Elements.Values)
                {

                    RetStr = RetStr + element.Name + "/";

                    // Get the complex type of the Customer element.
                    XmlSchemaComplexType complexType = element.ElementSchemaType as XmlSchemaComplexType;

                    // Get the sequence particle of the complex type.
                    XmlSchemaSequence sequence = complexType.ContentTypeParticle as XmlSchemaSequence;

                    // Iterate over each XmlSchemaElement in the Items collection.
                    foreach (XmlSchemaElement childElement in sequence.Items)
                    {
                        RetStr = RetStr + childElement.Name;
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
            return RetStr;
        }

        /// <summary>
        /// Читает XSD схему по заданному пути и возвращает строку с именем самого детального элемента, атрибуты которого надо читать
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        protected string GetDetalNodes(string path)
        {
            string RetStr = "";
            try
            {

                XmlTextReader reader = new XmlTextReader(path);
                XmlSchema myschema = XmlSchema.Read(reader, ValidationCallback);
                XmlSchemaSet schemaSet = new XmlSchemaSet();
                schemaSet.ValidationEventHandler += new ValidationEventHandler(ValidationCallback);
                schemaSet.Add(myschema);
                schemaSet.Compile();

                foreach (XmlSchemaElement element in myschema.Elements.Values)
                {
                    XmlSchemaComplexType complexType = element.ElementSchemaType as XmlSchemaComplexType;
                    XmlSchemaSequence sequence = complexType.ContentTypeParticle as XmlSchemaSequence;
                    foreach (XmlSchemaElement childElement in sequence.Items)
                    {
                        RetStr = childElement.Name;
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
            return RetStr;
        }

        /// <summary>
        /// Читает XSD схему по заданному пути и возвращает хеш-таблицу "имя поля"-"тип (размерность)"
        /// </summary>
        /// <param name="path">Путь до XSD-файла</param>
        /// <returns>hashtable</returns>
        protected Hashtable ParseXSD(string path)
        {
            Hashtable Ret = new Hashtable();

            try
            {
                XmlTextReader reader = new XmlTextReader(path);
                XmlSchema myschema = XmlSchema.Read(reader, ValidationCallback);
                XmlSchemaSet schemaSet = new XmlSchemaSet();
                schemaSet.ValidationEventHandler += new ValidationEventHandler(ValidationCallback);
                schemaSet.Add(myschema);
                schemaSet.Compile();

                foreach (XmlSchemaElement element in myschema.Elements.Values)
                {
                    // Get the complex type of the Customer element.
                    XmlSchemaComplexType complexType = element.ElementSchemaType as XmlSchemaComplexType;

                    // Get the sequence particle of the complex type.
                    XmlSchemaSequence sequence = complexType.ContentTypeParticle as XmlSchemaSequence;

                    // Iterate over each XmlSchemaElement in the Items collection.
                    foreach (XmlSchemaElement childElement in sequence.Items)
                    {
                        XmlSchemaComplexType childComplexType = childElement.ElementSchemaType as XmlSchemaComplexType;

                        // If the complex type has any attributes, get an enumerator 
                        // and write each attribute name to the console.
                        if (childComplexType.AttributeUses.Count > 0)
                        {
                            IDictionaryEnumerator enumerator = childComplexType.AttributeUses.GetEnumerator();

                            while (enumerator.MoveNext())
                            {
                                XmlSchemaAttribute childAttribute = (XmlSchemaAttribute)enumerator.Value;

                                if (!childAttribute.SchemaTypeName.IsEmpty)
                                {
                                    Ret.Add(childAttribute.Name, dataTabConversion(childAttribute.SchemaTypeName.Name));
                                }
                                else
                                {
                                    XmlSchemaSimpleType simpleType = childAttribute.AttributeSchemaType as XmlSchemaSimpleType;
                                    XmlSchemaSimpleTypeRestriction restriction = simpleType.Content as XmlSchemaSimpleTypeRestriction;

                                    Ret.Add(childAttribute.Name, dataTabConversion(restriction.BaseTypeName.Name));
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

            return Ret;
        }

        /// <summary>
        /// Читает XSD схему по заданному пути и возвращает словарь "имя поля"-"системный тип"
        /// </summary>
        /// <param name="path">Путь до XSD-файла</param>
        /// <returns>Dictionary<string, Type></returns>
        protected Dictionary<string, Type> ParseXSD2(string path)
        {
            Dictionary<string, Type> Ret = new Dictionary<string, Type>();

            try
            {
                XmlTextReader reader = new XmlTextReader(path);
                XmlSchema myschema = XmlSchema.Read(reader, ValidationCallback);
                XmlSchemaSet schemaSet = new XmlSchemaSet();
                schemaSet.ValidationEventHandler += new ValidationEventHandler(ValidationCallback);
                schemaSet.Add(myschema);
                schemaSet.Compile();

                foreach (XmlSchemaElement element in myschema.Elements.Values)
                {
                    // Get the complex type of the Customer element.
                    XmlSchemaComplexType complexType = element.ElementSchemaType as XmlSchemaComplexType;

                    // Get the sequence particle of the complex type.
                    XmlSchemaSequence sequence = complexType.ContentTypeParticle as XmlSchemaSequence;

                    // Iterate over each XmlSchemaElement in the Items collection.
                    foreach (XmlSchemaElement childElement in sequence.Items)
                    {
                        XmlSchemaComplexType childComplexType = childElement.ElementSchemaType as XmlSchemaComplexType;

                        // If the complex type has any attributes, get an enumerator 
                        // and write each attribute name to the console.
                        if (childComplexType.AttributeUses.Count > 0)
                        {
                            IDictionaryEnumerator enumerator = childComplexType.AttributeUses.GetEnumerator();

                            while (enumerator.MoveNext())
                            {
                                XmlSchemaAttribute childAttribute = (XmlSchemaAttribute)enumerator.Value;

                                if (!childAttribute.SchemaTypeName.IsEmpty)
                                {
                                    Ret.Add(childAttribute.Name, dataTabConversion(childAttribute.SchemaTypeName.Name));
                                }
                                else
                                {
                                    XmlSchemaSimpleType simpleType = childAttribute.AttributeSchemaType as XmlSchemaSimpleType;
                                    XmlSchemaSimpleTypeRestriction restriction = simpleType.Content as XmlSchemaSimpleTypeRestriction;

                                    Ret.Add(childAttribute.Name, dataTabConversion(restriction.BaseTypeName.Name));
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

            return Ret;
        }

        /// <summary>
        /// Принимает параметр
        /// </summary>
        /// <param name="element">имя элемента</param>
        /// <param name="el_size">размер элемента</param>
        /// <returns></returns>
        private string TabConversion(string element, string el_size = null)
        {
            switch (element)
            {
                case "string":
                    return "[varchar]" + "(" + el_size + ")";
                    //break;
                case "integer":
                    return "[int]";
                    //break;
                case "byte":
                    return "[int]";
                    //break;
                case "date":
                    return "[datetime]";
                    //break;
                default:
                    return "";
                    //break;
            }
        }

        /// <summary>
        /// Принимает строковой параметр и взависимости от его типа возвращает системный тип
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        private Type dataTabConversion(string element)
        {
            switch (element)
            {
                case "string":
                    return Type.GetType("System.String");
                //break;
                case "integer":
                    return Type.GetType("System.Int32");
                //break;
                case "byte":
                    return Type.GetType("System.Int32");
                //break;
                case "date":
                    return Type.GetType("System.DateTime");
                //break;
                default:
                    return null;
                    //    break;
            }
        }

        public bool isValidXML(string xmlpath, string xsdpath)
        {

            XmlSchemaSet sc = new XmlSchemaSet();

            // Add the schema to the collection.
            sc.Add(null, xsdpath);

            // Set the validation settings.
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.ValidationType = ValidationType.Schema;
            settings.Schemas = sc;
            settings.ValidationEventHandler += new ValidationEventHandler(ValidationCallback);

            // Create the XmlReader object.
            System.Xml.XmlReader reader = System.Xml.XmlReader.Create(xmlpath, settings);

            // Parse the file. 
            while (reader.Read()) { };

            return isValid;
        }

        //Дописать создание БД из XSD
        protected void CreateCheckDB(string path)
        {
            Dictionary<string, Type> ht = ParseXSD2(path);
            StringBuilder SQLText = new StringBuilder();

            foreach (KeyValuePair<string, Type> str in ht)
            {
                //TODO: Дописать проверку структуры БД, собрать команду и передать классу SQL
                //SQLText.Append("CREATE TABLE"+tab_name);
                //Console.WriteLine("Key = {0}, Value = {1}", de.Key, de.Value);
            }

        }

    }

    class MyStream : Stream, IDisposable
    {
        public MyStream(Stream innerStream)
        {
            InnerStream = innerStream;
        }

        public Stream InnerStream;

        public override void Close()
        {
            InnerStream.Close();
        }

        void IDisposable.Dispose()
        {
            base.Dispose();
            InnerStream.Dispose();
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = InnerStream.Read(buffer, offset, count);
            OnPositionChanged();
            return read;
        }

        public override int ReadByte()
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override void WriteByte(byte value)
        {
            throw new NotImplementedException();
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanTimeout
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { return InnerStream.Length; }
        }

        public override long Position
        {
            get { return InnerStream.Position; }
            set { throw new NotImplementedException(); }
        }

        public event EventHandler PositionChanged;

        protected virtual void OnPositionChanged()
        {
            if (PositionChanged != null)
            {
                PositionChanged(this, EventArgs.Empty);
            }
        }
    }
}
