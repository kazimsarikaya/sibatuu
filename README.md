# SIBATUU (SImple BAckup TUUl)

SIBATUU is a simple backup tool for creating backups to a local or remote (with s3 api) location and restore it. Configuration can be given as config file,
environment variable or command line parameter.

Configuration file can be json, yaml or toml. Configuration file will be parsed by viper. Environment variables should
be start by ```SIBATUU_``` and auto detected by argument name such as ```SIBATUU_REPOSITORY```.

If repository is at local file system, repository will be a path address such as ```/my/backup/repo```. If repository
address is at S3 compatible object storage, format will be like ```s3://<username>:<password>@<host>:<port>/<bucket>[/sub/path]```.

# Usage

## sibatuu

A simple backup tool

### Synopsis

A beatiful backup and restore tool supporting local file systems and s3

### Options

```
  -c, --cache string        local cache directory
      --config string       configuration file
  -h, --help                help for sibatuu
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu backup](#sibatuu-backup)	 - Backups given path to the repository
* [sibatuu init](#sibatuu-init)	 - Initialize a backup repository
* [sibatuu list](#sibatuu-list)	 - List backups
* [sibatuu restore](#sibatuu-restore)	 - Restore full backup or single file
* [sibatuu version](#sibatuu-version)	 - Show version information

## sibatuu backup

Backups given path to the repository

### Synopsis

Backups given path, as parameter source, to the repository.
Command uses a local cache directory for metadata. Also a tag can be given.
If tag not given it's default is backup timestamp.

```
sibatuu backup [flags]
```

### Options

```
  -h, --help            help for backup
  -s, --source string   Backup source which will be backuped
  -t, --tag string      Backup tag, used for identify backup sources
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu help

Help about any command

### Synopsis

Help provides help for any command in the application.
Simply type sibatuu help [path to command] for full details.

```
sibatuu help [command] [flags]
```

### Options

```
  -h, --help   help for help
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu init

Initialize a backup repository

### Synopsis

Initialze a backup repository with given path (-r)
repository path can be start with file: or s3:, if not given default is file.
Configuration can be given from config file, as parameter of envrionment variables.
The apply order is sames as description

```
sibatuu init [flags]
```

### Options

```
  -h, --help   help for init
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu list

List backups

### Synopsis

List backups. If no parameters given list all backup information.
If latest parameter given, filters backups with tag as prefix and display latest backup content.
If tag given lists backups with prefixed with that tag.
If backup id given, list contents of that backup.

```
sibatuu list [flags]
```

### Options

```
  -i, --backup-id uint   Backup id for listing given backup
  -d, --detail           Show compress and dedup ratio (slow operation)
  -h, --help             help for list
  -l, --latest           List latest backup. if tag given filter by tag prefix
  -t, --tag string       Backup tag for listing given backup
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu readme



```
sibatuu readme [flags]
```

### Options

```
  -h, --help   help for readme
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu restore

Restore full backup or single file

### Synopsis

Restores full backup or single file.
If only backup id or backup tag given, restores full backup.
If file id or file name given, restores single backup.
If latest parameter given, filters backup with tag as prefix and restore operation performed on latest of filtered backups.

```
sibatuu restore [flags]
```

### Options

```
  -i, --backup-id uint       Backup id for restoring given backup
  -d, --destination string   Backup restore destination directory
  -f, --file-id int          File id for restore single file (default -1)
  -n, --file-name string     File name for restore single file
  -h, --help                 help for restore
  -o, --override             Override if file exists at destination
  -t, --tag string           Backup tag for listing given backup
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

## sibatuu version

Show version information

```
sibatuu version [flags]
```

### Options

```
  -h, --help   help for version
```

### Options inherited from parent commands

```
  -c, --cache string        local cache directory
      --config string       configuration file
      --logtostderr         log to standard error instead of files (default true)
  -r, --repository string   backup repository
  -v, --v Level             number for the log level verbosity
```

### SEE ALSO

* [sibatuu](#sibatuu)	 - A simple backup tool

