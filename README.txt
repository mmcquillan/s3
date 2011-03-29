s3 is a windows command line application to allow FTP style access to
Amazon's simple storage solution. Commands can be passed as parameters
or run the application in interactive mode.

-----------------------------------------------------------------------------

S3 Commands:

  ls [bucket] [key] [marker]
  size {bucket}
  csv {bucket}
  mkdir {bucket}
  rmdir {bucket}
  rm {bucket} {file}
  put {bucket} {file} [key] [acl]
  mput {bucket} {directory} [file filter: *.xaml]
  mputr {bucket} {directory} {file filter: *.xaml}
  mputl {bucket} {input file}
  get {bucket} {key} [output directory]
  mgetl {bucket} {input file} [output directory]
  acl {AuthenticatedRead | BucketOwnerFullControl | BucketOwnerRead |
                       NoACL | Private | PublicRead | PublicReadWrite}
  thread {max thread count}
  retry {max retry count}
  exit