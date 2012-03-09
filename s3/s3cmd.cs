using System;
using System.IO;
using System.Collections;
using System.Xml;
using System.Threading;
using System.Net;
using System.Text;
using System.Security.Cryptography;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3
{
	class s3cmd
	{

		private AmazonS3 s3Client;
        private string awsKey;
        private string awsSecret;
        private int maxThreads = 0;
        private int maxRetry = 3;
	    private string ACL = "";
        private int putTimeout = 0;

        public s3cmd(string awsAccessKeyId, string awsSecretAccessKey)
		{
			// setup configuration
            this.awsKey = awsAccessKeyId;
            this.awsSecret = awsSecretAccessKey;
            this.s3Client = AWSClientFactory.CreateAmazonS3Client(awsKey, awsSecret);

            // configuration
            ServicePointManager.DefaultConnectionLimit = 20;

		}

        public void ls()
        {
            try
            {
                ListBucketsResponse list = this.s3Client.ListBuckets();
                foreach (S3Bucket bucket in list.Buckets)
                {
                    Console.WriteLine(bucket.BucketName);
                }
                
            }
            catch (Exception e)
            {
                Console.WriteLine("Error listing buckets: " + e.Message);
            }
        }

		public void ls(string bucket)
		{
            string nextmarker = "";
            while(nextmarker != null)
            {
                nextmarker = ls(bucket, nextmarker);
            }
		}

        public string ls(string bucket, string marker)
        {
            try
            {
                ListObjectsRequest listrequest = new ListObjectsRequest();
                listrequest.BucketName = bucket;
                listrequest.Marker = marker;
                ListObjectsResponse listresponse = s3Client.ListObjects(listrequest);
                foreach (S3Object s3obj in listresponse.S3Objects)
                {
                    Console.WriteLine(s3obj.Key);
                }
                if (listresponse.S3Objects.Count == 1000)
                {
                    marker = listresponse.S3Objects[999].Key;
                }
                else
                {
                    marker = null;
                }
                return marker;
            }
            catch (Exception e)
            {
                // report error and retry
                Console.WriteLine("Error retrieving bucket list for bucket '" + bucket + "': " + e.Message);
                if (marker != null && marker != "")
                {
                    return ls(bucket, marker);
                }
                else
                {
                    return null;
                }
            }
        }

        public void csv(string bucket)
        {
            string nextmarker = "";
            try
            {
                Console.Write("\"BUCKET_NAME\"");
                Console.Write(",");
                Console.Write("\"KEY_NAME\"");
                Console.Write(",");
                Console.Write("\"LAST_MODIFIED\"");
                Console.Write(",");
                Console.WriteLine("\"SIZE_BYTES\"");
                while (nextmarker != null)
                {
                    ListObjectsRequest listrequest = new ListObjectsRequest();
                    listrequest.BucketName = bucket;
                    listrequest.Marker = nextmarker;
                    ListObjectsResponse listresponse = s3Client.ListObjects(listrequest);
                    foreach (S3Object s3obj in listresponse.S3Objects)
                    {
                        Console.Write("\"" + bucket + "\"");
                        Console.Write(",");
                        Console.Write("\"" + s3obj.Key + "\"");
                        Console.Write(",");
                        Console.Write("\"" + DateTime.Parse(s3obj.LastModified).ToString() + "\"");
                        Console.Write(",");
                        Console.WriteLine(s3obj.Size);
                    }
                    if (listresponse.S3Objects.Count == 1000)
                    {
                        nextmarker = listresponse.S3Objects[999].Key;
                    }
                    else
                    {
                        nextmarker = null;
                    }
                }
            }
            catch (Exception e)
            {
                // report error and retry
                Console.WriteLine("Error retrieving bucket list for bucket '" + bucket + "': " + e.Message);
            }
        }

        public void size(string bucket)
        {
            int count = 0;
            long filesize = 0;
            string nextmarker = "";
            try
            {
                while (nextmarker != null)
                {
                    ListObjectsRequest listrequest = new ListObjectsRequest();
                    listrequest.BucketName = bucket;
                    listrequest.Marker = nextmarker;
                    ListObjectsResponse listresponse = s3Client.ListObjects(listrequest);
                    foreach (S3Object s3obj in listresponse.S3Objects)
                    {
                        count++;
                        filesize += s3obj.Size;
                    }
                    if (listresponse.S3Objects.Count == 1000)
                    {
                        nextmarker = listresponse.S3Objects[999].Key;
                    }
                    else
                    {
                        nextmarker = null;
                    }
                }
                Console.WriteLine("Bucket: " + bucket);
                Console.WriteLine("File Count: " + count.ToString());
                Console.WriteLine("Total File Size: " + filesize.ToString());
            }
            catch (Exception e)
            {
                // report error and retry
                Console.WriteLine("Error retrieving bucket list for bucket '" + bucket + "': " + e.Message);
            }
        }

		public void mkdir(string bucket)
		{
			try
			{
                PutBucketRequest putbucketrequest = new PutBucketRequest();
                putbucketrequest.BucketName = bucket;
                s3Client.PutBucket(putbucketrequest);
                Console.WriteLine("Bucket '" + bucket + "' has been created");
			}
			catch(Exception e)
			{
				Console.WriteLine("Error creating bucket '" + bucket + "': " + e.Message);
			}
		}

		public void rmdir(string bucket)
		{
			try
			{
                DeleteBucketRequest deletebucketrequest = new DeleteBucketRequest();
                deletebucketrequest.BucketName = bucket;
                s3Client.DeleteBucket(deletebucketrequest);
				Console.WriteLine("Bucket '" + bucket + "' has been deleted");
			}
			catch ( Exception e )
			{
				Console.WriteLine("Error deleting bucket '" + bucket + "': " + e.Message);
			}
		}

		public void rm(string bucket, string key)
		{
			try
			{
                DeleteObjectRequest deleteobjectrequest = new DeleteObjectRequest();
                deleteobjectrequest.BucketName = bucket;
                deleteobjectrequest.Key = key;
                s3Client.DeleteObject(deleteobjectrequest);
				Console.WriteLine("File '" + key + "' in bucket '" + bucket + "' has been deleted");
			}
			catch ( Exception e )
			{
				Console.WriteLine("Error deleting file '" + key + "' in bucket '" + bucket + "': " + e.Message);
			}
		}

		public void put(string bucket, string file)
		{
			string key = file.Substring(file.LastIndexOf('\\') + 1);
			this.put(bucket, file, key);
		}

		public void put(string bucket, string file, string key)
		{
            s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, ACL, putTimeout);
            put.Run(null);
		}

        public void put(string bucket, string file, string key, string acl)
        {
            s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, acl, putTimeout);
            put.Run(null);
        }

		public void mput(string bucket, string dir)
		{
			try
			{
				// get all files
				string[] files = Directory.GetFiles(dir);

                // for threading library
                ThreadPool.SetMaxThreads(maxThreads, maxThreads);
                ThreadPoolWait threads = new ThreadPoolWait();

				// loop over and add to S3
				foreach (string file in files)
                {
                    string key = file.Substring(file.LastIndexOf('\\') + 1);
                    s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, ACL, putTimeout);
                    threads.QueueUserWorkItem(new WaitCallback(put.Run));
				}

                // wait for all threads to complete
                threads.WaitOne();
			}
			catch ( Exception e )
			{
				Console.WriteLine("Error in: '" + dir + "': " + e.Message);
			}
		}

		public void mput(string bucket, string dir, string filter)
		{
			try
			{
				// get all files
				string[] files = Directory.GetFiles(dir, filter);

                // for threading library
                ThreadPool.SetMaxThreads(maxThreads, maxThreads);
                ThreadPoolWait threads = new ThreadPoolWait();

                // loop over and add to S3
                foreach (string file in files)
                {
                    string key = file.Substring(file.LastIndexOf('\\') + 1);
                    s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, ACL, putTimeout);
                    threads.QueueUserWorkItem(new WaitCallback(put.Run));
                }

                // wait for all threads to complete
                threads.WaitOne();
			}
			catch ( Exception e )
			{
				Console.WriteLine("Error in: '" + dir + "': " + e.Message);
			}
		}

		public void mputr(string bucket, string dir, string filter)
		{
			try
			{
				// get all files
				ArrayList files = GetFilesRecursive(dir, filter);

                // for threading library
                ThreadPool.SetMaxThreads(maxThreads, maxThreads);
                ThreadPoolWait threads = new ThreadPoolWait();

                // loop over and add to S3
                foreach (string file in files)
                {
                    string key = file.Substring(file.LastIndexOf('\\') + 1);
                    s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, ACL, putTimeout);
                    threads.QueueUserWorkItem(new WaitCallback(put.Run));
                }

                // wait for all threads to complete
                threads.WaitOne();
			}
			catch(Exception e)
			{
				Console.WriteLine("Error in: '" + dir + "': " + e.Message);
			}
		}

        public void mputl(string bucket, string inputfile)
        {
            try
            {

                // open input file
                StreamReader files = new StreamReader(inputfile);

                // for threading library
                ThreadPool.SetMaxThreads(maxThreads, maxThreads);
                ThreadPoolWait threads = new ThreadPoolWait();

                // loop over and add to S3
                string file;
                while((file = files.ReadLine()) != null)
                {
                    if(file.Trim() != "")
                    {
                        string key = file.Substring(file.LastIndexOf('\\') + 1);
                        s3Put put = new s3Put(awsKey, awsSecret, bucket, file, key, maxRetry, ACL, putTimeout);
                        threads.QueueUserWorkItem(new WaitCallback(put.Run));
                    }
                }

                // wait for all threads to complete
                threads.WaitOne();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in: '" + inputfile + "': " + e.Message);
            }
        }

        public void move(string bucket, string file)
        {
            string fileChecksum = GetFileChecksum(file);
            string key = file.Substring(file.LastIndexOf('\\') + 1);
            this.put(bucket, file, key);
            string s3Checksum = GetS3Checksum(bucket, key);
            if (fileChecksum == s3Checksum)
            {
                File.Delete(file);
                Console.WriteLine("Checksums match and the local file '" + file + "' is deleted.");
            }
            else
            {
                Console.WriteLine("Error: Checksums do not match and the local file '" + file + "' is not deleted.");
            }
        }

        public void get(string bucket, string key, string dir)
        {
            s3Get get = new s3Get(awsKey, awsSecret, bucket, dir, key, maxRetry);
            get.Run(null);
        }

        public void mgetl(string bucket, string inputfile, string outputdir)
        {
            try
            {

                // open input file
                StreamReader files = new StreamReader(inputfile);

                // for threading library
                ThreadPool.SetMaxThreads(maxThreads, maxThreads);
                ThreadPoolWait threads = new ThreadPoolWait();

                // loop over and add to S3
                string file;
                while ((file = files.ReadLine()) != null)
                {
                    if (file.Trim() != "")
                    {
                        string key = file.Substring(file.LastIndexOf('\\') + 1);
                        s3Get get = new s3Get(awsKey, awsSecret, bucket, outputdir, key, maxRetry);
                        threads.QueueUserWorkItem(new WaitCallback(get.Run));
                    }
                }

                // wait for all threads to complete
                threads.WaitOne();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in: '" + inputfile + "': " + e.Message);
            }
        }
        
        public void accesscontrol(string acl)
        {
            this.ACL = acl;
        }

        public void threads(int Threads)
        {
            this.maxThreads = Threads;
        }

        public void retry(int Retry)
        {
            this.maxRetry = Retry;
        }

        public void timeout(int Timeout)
        {
            this.putTimeout = Timeout;
        }

		// recusive directory listing
		private ArrayList GetFilesRecursive(string basedirectory, string pattern)
		{
			// init array
			ArrayList allfiles = new ArrayList();

			// get file listings
			string[] files = Directory.GetFiles(basedirectory, pattern);

			// loop over files
			foreach ( string file in files )
			{
				allfiles.Add(file);
			}

			// get directory listings
			string[] dirs = Directory.GetDirectories(basedirectory);
			foreach ( string dir in dirs )
			{
				allfiles.AddRange(GetFilesRecursive(dir, pattern));
			}

			return allfiles;
		}

        // md5 file cheksum
        private string GetFileChecksum(string file)
        {
            using (FileStream stream = File.OpenRead(file))
            {
                MD5 md5 = new MD5CryptoServiceProvider();
                byte[] checksum = md5.ComputeHash(stream);
                return BitConverter.ToString(checksum).Replace("-", String.Empty).ToUpper();
            }
        }

        // md5 s3 checksum
        private string GetS3Checksum(string bucket, string key)
        {
            GetObjectMetadataRequest req = new GetObjectMetadataRequest();
            req.BucketName = bucket;
            req.Key = key;
            GetObjectMetadataResponse res = new GetObjectMetadataResponse();
            res = s3Client.GetObjectMetadata(req);
            return res.ETag.ToUpper().Replace("\"", "");
        }

	}
}