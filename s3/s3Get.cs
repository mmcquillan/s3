using System;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3
{
    class s3Get
    {
        // properties
        private string awsKey;
        private string awsSecret;
        private string bucket;
        private string file;
        private string key;
        private int retry;
        
        // constructer
        public s3Get(string awsAccessKeyId, string awsSecretAccessKey, string Bucket, string Dir, string Key, int Retry)
        {
            // set vars
            awsKey = awsAccessKeyId;
            awsSecret = awsSecretAccessKey;
            bucket = Bucket;
            key = Key;
            retry = Retry;

            // set the file to save
            if (Dir.EndsWith("\\"))
            {
                file = Dir + Key;
            }
            else
            {
                file = Dir + "\\" + Key;
            }
        }

        // methods
        public void Run(Object obj)
        {

            // counter for retires
            if (retry > 0)
            {

                // make the amazon client
                AmazonS3 s3Client = AWSClientFactory.CreateAmazonS3Client(awsKey, awsSecret);

                try
                {

                    // request object with file
                    GetObjectRequest req = new GetObjectRequest();
                    req.BucketName = bucket;
                    req.Key = key;

                    // get the object from s3
                    GetObjectResponse res = s3Client.GetObject(req);

                    // test for paths in key name
                    if (file.IndexOf('/') > 0)
                    {
                        string filepath = file.Remove(file.LastIndexOf('/')).Replace('/', '\\');
                        if (!Directory.Exists(filepath))
                        {
                            Directory.CreateDirectory(filepath);
                        }
                    }


                    // establish a local file to send to
                    FileStream fs = File.Create(file.Replace('/', '\\'));

                    // transfer the stream to a file
                    byte[] buffer = new byte[8 * 1024];
                    int len;
                    while ((len = res.ResponseStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        fs.Write(buffer, 0, len);
                    }
                    
                    // close streams
                    res.ResponseStream.Close();
                    fs.Close();

                    // feedback
                    Console.WriteLine("File '" + key + "' in bucket '" + bucket + "' has been downloaded");

                }
                catch (Exception e)
                {

                    // output error
                    Console.WriteLine("Warning getting file '" + key + "' in bucket '" + bucket + "': " + e.Message + " (operation will be retried)");
                    
                    // lower the counter and rerun
                    retry = retry - 1;
                    Run(obj);

                }

            }
            else
            {

                // exceeded retries, time to output error
                Console.WriteLine("Error adding file '" + file + "' in bucket '" + bucket + "'");

            }

        }

    }
}
