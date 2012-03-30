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
        private string bucket;
        private string file;
        private string key;
        private int retry;
        private string acl;
        private int timeout;
        
        // constructer
        public s3Put(string awsAccessKeyId, string awsSecretAccessKey, string Bucket, string File, string Key, int Retry, string ACL, int PutTimeout)
        {
            awsKey = awsAccessKeyId;
            awsSecret = awsSecretAccessKey;
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
                AmazonS3 s3Client = AWSClientFactory.CreateAmazonS3Client(awsKey, awsSecret);

                try
                {

                    // request object with file
                    PutObjectRequest req = new PutObjectRequest();
                    req.BucketName = bucket;
                    req.Key = key;
                    req.FilePath = file;
                    req.CannedACL = ACL();
                    req.Timeout = 120 * 60 * 1000;
                    
                    // set the appropriate mime type
                    if (file.Contains("."))
                    {
                        string ext = file.Substring(file.LastIndexOf('.'));
                        string mime = AmazonS3Util.MimeTypeFromExtension(ext);
                        req.AddHeader("content-type", mime);
                    }

                    // set timeout
                    if(timeout > 0)
                    {
                        req.Timeout = timeout;
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
                    break;
                case "BucketOwnerFullControl":
                    return S3CannedACL.BucketOwnerFullControl;
                    break;
                case "BucketOwnerRead":
                    return S3CannedACL.BucketOwnerRead;
                    break;
                case "NoACL":
                    return S3CannedACL.NoACL;
                    break;
                case "Private":
                    return S3CannedACL.Private;
                    break;
                case "PublicRead":
                    return S3CannedACL.PublicRead;
                    break;
                case "PublicReadWrite":
                    return S3CannedACL.PublicReadWrite;
                    break;
                default:
                    return S3CannedACL.NoACL;
                    break;
            }
        }

    }
}
