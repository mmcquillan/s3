using System;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using System.Threading;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

namespace s3
{
    class s3Put
    {
        // properties
        private string awsKey;
        private string awsSecret;
        private string region;
        private string bucket;
        private string file;
        private string key;
        private int retry;
        private string acl;
        private int timeout;
        
        // constructer
        public s3Put(string awsAccessKeyId, string awsSecretAccessKey, string Region, string Bucket, string File, string Key, int Retry, string ACL, int PutTimeout)
        {
            awsKey = awsAccessKeyId;
            awsSecret = awsSecretAccessKey;
            region = Region;
            bucket = Bucket;
            file = File;
            key = Key;
            retry = Retry;
            acl = ACL;
            timeout = PutTimeout;
        }

        // methods
        public void Run(Object obj)
        {

            // counter for retires
            if (retry > 0)
            {

                // make the amazon client
                AmazonS3Client s3Client = new AmazonS3Client(awsKey, awsSecret, RegionEndpoint.GetBySystemName(region));

                try
                {

                    // request object with file
                    PutObjectRequest req = new PutObjectRequest();
                    req.BucketName = bucket;
                    req.Key = key;
                    req.FilePath = file;
                    req.CannedACL = ACL();
                    req.Timeout = new TimeSpan(2, 0, 0);
                    
                    // set the appropriate mime type
                    if (file.Contains("."))
                    {
                        string ext = file.Substring(file.LastIndexOf('.'));
                        string mime = AmazonS3Util.MimeTypeFromExtension(ext);
                        req.Headers.ContentType = mime;
                    }

                    // set timeout
                    if(timeout > 0)
                    {
                        req.Timeout = new TimeSpan(0, 0, timeout);
                    }

                    // put the object
                    PutObjectResponse res = s3Client.PutObject(req);

                    // feedback
                    Console.WriteLine("File '" + key + "' in bucket '" + bucket + "' has been added");

                }
                catch (Exception e)
                {

                    // output error
                    Console.WriteLine("Warning adding file '" + file + "' in bucket '" + bucket + "': " + e.Message + " (operation will be retried)");
                    
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

        private S3CannedACL ACL()
        {
            switch(acl)
            {
                case "AuthenticatedRead":
                    return S3CannedACL.AuthenticatedRead;
                case "BucketOwnerFullControl":
                    return S3CannedACL.BucketOwnerFullControl;
                case "BucketOwnerRead":
                    return S3CannedACL.BucketOwnerRead;
                case "NoACL":
                    return S3CannedACL.NoACL;
                case "Private":
                    return S3CannedACL.Private;
                case "PublicRead":
                    return S3CannedACL.PublicRead;
                case "PublicReadWrite":
                    return S3CannedACL.PublicReadWrite;
                default:
                    return S3CannedACL.NoACL;
            }
        }

    }
}
