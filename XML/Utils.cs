using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Xml.Schema;
using System.Text.RegularExpressions;

namespace XML
{
    static class Utils
    {
        static bool isValid = true;

        public static string[] GetFiles(string root)
        {
            string[] files = null;
            Stack<string> dirs = new Stack<string>(20);

            //if (!System.IO.Directory.Exists(root))
            //{
            //    throw new ArgumentException();
            //}
            dirs.Push(root);

            while (dirs.Count > 0)
            {
                string currentDir = dirs.Pop();
                string[] subDirs;
                try
                {
                    subDirs = Directory.GetDirectories(currentDir);
                }
                catch (UnauthorizedAccessException e)
                {
                    throw new UnauthorizedAccessException(e.Message);
                }
                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                try
                {
                    files = Directory.GetFiles(currentDir);
                }

                catch (UnauthorizedAccessException e)
                {

                    throw new UnauthorizedAccessException(e.Message);
                }

                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                foreach (string str in subDirs)
                    dirs.Push(str);
            }

            return files;
        }

        public static Dictionary<string, long> GetFiles2(string root)
        {
            Dictionary<string, long> allfiles = new Dictionary<string, long>();

            string[] files = null;
            Stack<string> dirs = new Stack<string>(20);

            //if (!System.IO.Directory.Exists(root))
            //{
            //    throw new ArgumentException();
            //}
            dirs.Push(root);

            while (dirs.Count > 0)
            {
                string currentDir = dirs.Pop();
                string[] subDirs;
                try
                {
                    subDirs = Directory.GetDirectories(currentDir);
                }
                catch (UnauthorizedAccessException e)
                {
                    throw new UnauthorizedAccessException(e.Message);
                }
                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                try
                {
                    files = Directory.GetFiles(currentDir);
                    foreach (string f in files)
                    {
                        FileInfo file = new FileInfo(f);
                        allfiles.Add(f, file.Length);
                    }
                }

                catch (UnauthorizedAccessException e)
                {

                    throw new UnauthorizedAccessException(e.Message);
                }

                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                foreach (string str in subDirs)
                    dirs.Push(str);
            }

            return allfiles;
        }

        public static Dictionary<string, string> GetShortFiles(string root)
        {
            Dictionary<string, string> allfiles = new Dictionary<string, string>();
            string[] files = null;
            Stack<string> dirs = new Stack<string>(20);

            dirs.Push(root);

            while (dirs.Count > 0)
            {
                string currentDir = dirs.Pop();
                string[] subDirs;
                try
                {
                    subDirs = Directory.GetDirectories(currentDir);
                }
                catch (UnauthorizedAccessException e)
                {
                    throw new UnauthorizedAccessException(e.Message);
                }
                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                try
                {
                    //TODO: 
                    files = Directory.GetFiles(currentDir);
                    foreach (string f in files)
                    {
                        string pattern = @"_[^_]*_[^_]*\.XML$";
                        string replacement = ".XML";
                        Regex newReg = new Regex(pattern);
                        allfiles.Add(newReg.Replace(f, replacement), f);
                    }
                }

                catch (UnauthorizedAccessException e)
                {

                    throw new UnauthorizedAccessException(e.Message);
                }

                catch (DirectoryNotFoundException e)
                {
                    throw new DirectoryNotFoundException(e.Message);
                }

                foreach (string str in subDirs)
                    dirs.Push(str);
            }

            return allfiles;
        }

        private static void ValidationCallback(object sender, ValidationEventArgs args)
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

        public static bool isValidXML(string xmlpath, string xsdpath)
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

        public static long GetFileSize(string fName)
        {
            FileInfo file = new FileInfo(fName);

            return file.Length;
        }

        public static string[] GetDateVer(string path)
        {
            string[] RetArray = new string[2];
            string pattern = @"[^_]*\d";
            Regex newReg = new Regex(pattern);
            MatchCollection matches = newReg.Matches(path);
            if (matches.Count == 2)
            {
                RetArray[0] = matches[0].Value;
                RetArray[1] = matches[1].Value;
            }
            return RetArray;
        }

        public static string GetTabName(string path)
        {
            string pattern = @"[^_]*";
            Regex newReg = new Regex(pattern);
            MatchCollection matches = newReg.Matches(path);

            return matches[0].Value;
        }
    }
}

