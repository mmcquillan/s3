using System;
using System.Text;
using System.Configuration;
using System.Collections.Specialized;

namespace s3
{
	class Program
	{

        public static NameValueCollection config;
        private static s3cmd s3;

		static void Main(string[] args)
		{
            
            // initialize app
            config = ConfigurationManager.AppSettings;
            s3 = new s3cmd(config["AWSAccessKey"], config["AWSSecretKey"]);
            s3.accesscontrol(config["ACL"]);
            s3.threads(Int32.Parse(config["Threads"]));
            s3.retry(Int32.Parse(config["Retries"]));
            s3.timeout(Int32.Parse(config["PutTimeout"]));

            // interpret input comand
			if ( args.Length > 0 )
			{
				StringBuilder command = new StringBuilder("");
				foreach(string a in args)
				{
					command.Append(a + " ");
				}
				Cmd(command.ToString());
			}
			else
			{
				while ( true )
				{
					Console.Write("S3> ");
					string cmd = Console.ReadLine();
					Cmd(cmd);
				}
			}

		}
        
		private static void Cmd(string command)
		{
			// interpret command
			char[] s = new char[1];
			s[0] = ' ';
			string[] cmd = command.Trim().Split(s);

			// take input
			switch ( cmd[0] )
			{
				case "ls":
					switch (cmd.Length)
					{
                        case 1:
                            s3.ls();
                            break;
						case 2:
							s3.ls(cmd[1]);
							break;
						case 3:
							s3.ls(cmd[1], cmd[2]);
							break;
						default:
							Help();
							break;
					}
					break;
                case "size":
                    switch (cmd.Length)
                    {
                        case 2:
                            s3.size(cmd[1]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "csv":
                    switch (cmd.Length)
                    {
                        case 2:
                            s3.csv(cmd[1]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
				case "mkdir":
					switch ( cmd.Length )
					{
						case 2:
							s3.mkdir(cmd[1]);
							break;
						default:
							Help();
							break;
					}
					break;
				case "rmdir":
					switch ( cmd.Length )
					{
						case 2:
							s3.rmdir(cmd[1]);
							break;
						default:
							Help();
							break;
					}
					break;
				case "rm":
					switch ( cmd.Length )
					{
						case 3:
							s3.rm(cmd[1], cmd[2]);
							break;
						default:
							Help();
							break;
					}
					break;
				case "put":
					switch ( cmd.Length )
					{
						case 3:
							s3.put(cmd[1], cmd[2]);
							break;
						case 4:
							s3.put(cmd[1], cmd[2], cmd[3]);
                            break;
                        case 5:
                            s3.put(cmd[1], cmd[2], cmd[3], cmd[4]);
                            break;
						default:
							Help();
							break;
					}
					break;
				case "mput":
					switch ( cmd.Length )
					{
						case 3:
							s3.mput(cmd[1], cmd[2]);
							break;
						case 4:
							s3.mput(cmd[1], cmd[2], cmd[3]);
							break;
						default:
							Help();
							break;
					}
					break;
				case "mputr":
					switch ( cmd.Length )
					{
						case 4:
							s3.mputr(cmd[1], cmd[2], cmd[3]);
							break;
						default:
							Help();
							break;
					}
                    break;
                case "mputl":
                    switch (cmd.Length)
                    {
                        case 3:
                            s3.mputl(cmd[1], cmd[2]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "move":
                    switch (cmd.Length)
                    {
                        case 3:
                            s3.move(cmd[1], cmd[2]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "get":
                    switch (cmd.Length)
                    {
                        case 3:
                            s3.get(cmd[1], cmd[2], Environment.CurrentDirectory);
                            break;
                        case 4:
                            s3.get(cmd[1], cmd[2], cmd[3]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "mgetl":
                    switch (cmd.Length)
                    {
                        case 3:
                            s3.mgetl(cmd[1], cmd[2], Environment.CurrentDirectory);
                            break;
                        case 4:
                            s3.mgetl(cmd[1], cmd[2], cmd[3]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "acl":
                    switch (cmd.Length)
                    {
                        case 2:
                            s3.accesscontrol(cmd[1]);
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "thread":
                    switch (cmd.Length)
                    {
                        case 2:
                            s3.threads(Int32.Parse(cmd[1]));
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
                case "retry":
                    switch (cmd.Length)
                    {
                        case 2:
                            s3.retry(Int32.Parse(cmd[1]));
                            break;
                        default:
                            Help();
                            break;
                    }
                    break;
				case "exit":
					Environment.Exit(0);
					break;
				default:
					Help();
					break;
			}
		}

		public static void Help()
		{
            Console.WriteLine("");
            Console.WriteLine("S3 Commands:");
            Console.WriteLine("  ls [bucket] [key] [marker]");
            Console.WriteLine("  size {bucket}");
            Console.WriteLine("  csv {bucket}");
			Console.WriteLine("  mkdir {bucket}");
			Console.WriteLine("  rmdir {bucket}");
			Console.WriteLine("  rm {bucket} {file}");
            Console.WriteLine("  put {bucket} {file} [key] [acl]");
			Console.WriteLine("  mput {bucket} {directory} [file filter: *.xaml]");
            Console.WriteLine("  mputr {bucket} {directory} {file filter: *.xaml}");
            Console.WriteLine("  mputl {bucket} {input file}");
            Console.WriteLine("  move {bucket} {file}");
            Console.WriteLine("  get {bucket} {key} [output directory]");
            Console.WriteLine("  mgetl {bucket} {input file} [output directory]");
		    Console.WriteLine("  acl {AuthenticatedRead | BucketOwnerFullControl | BucketOwnerRead | ");
            Console.WriteLine("                       NoACL | Private | PublicRead | PublicReadWrite}");
            Console.WriteLine("  thread {max thread count}");
            Console.WriteLine("  retry {max retry count}");
			Console.WriteLine("  exit");
			Console.WriteLine("");
		}

	}
}
