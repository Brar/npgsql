# This file contains portable functions to install and prepare various PostgreSQL
# versions to run the Npgsql tests in the GitHub CI setting.

$ErrorActionPreference = 'Stop'

# Here we set some constants and make sure that the PostgreSQL bin directory is in the PATH
# and the PGDATA environment variable is set correctly
if ($IsLinux) {
    Set-Variable PgDgKeyUrl -Option Constant -Value 'https://www.postgresql.org/media/keys/ACCC4CF8.asc'
    Set-Variable PgDgUrl -Option Constant -Value 'https://apt.postgresql.org/pub/repos/apt/'
    Set-Variable PgDgArchiveUrl -Option Constant -Value 'https://apt-archive.postgresql.org/pub/repos/apt/'
    # On Ubuntu postgresql is already running and the bin directory is already in the PATH
    # so we can reliably determine the data directory via psql
    $Env:PGDATA = sudo -u postgres psql -c "SHOW data_directory" -Atq -U postgres -d postgres
}
elseif ($IsWindows) {
    Set-Variable ApplicationsXmlUrl -Option Constant -Value 'https://sbp.enterprisedb.com/applications.xml'
    Set-Variable DownloadUrlTemplate -Option Constant -Value 'https://get.enterprisedb.com/postgresql/postgresql-{0}-windows-x64-binaries.zip'
    # On Windows the PGDATA environment variable is already set and PGBIN environment variable
    # is set to the bin directory so we prepend that one to the PATH
    $Env:PATH = $Env:PGBIN + ';' + $Env:PATH
}
elseif ($IsMacOS) {
    # On MacOs the bin directory is already in the PATH but PostgreSQL is not running.
    # We don't want to start it here just to determine the data directory so we use
    # the constant default and hope that it doesn't change too frequently
    $Env:PGDATA = "/usr/local/var/postgres"
}

function Install-PostgresUbuntuPgDg {
    Param(
        [Parameter(Mandatory=$True, Position=0)]
        [string]$Version,
        [Parameter(Mandatory=$True, Position=1)]
        [bool]$IsPrerelease
    )

    dpkg-query -W --showformat='${Package}\n' 'postgresql-*' | xargs sudo dpkg -P postgresql
    wget --quiet -O - $PgDgKeyUrl | sudo apt-key add -
    if ($IsPrerelease) {
        sudo add-apt-repository "deb $PgDgUrl $(lsb_release -cs)-pgdg-testing main $Version"
        "Package: *`nPin: release a=$(lsb_release -cs)-pgdg-testing`nPin-Priority: 500" | sudo tee /etc/apt/preferences.d/pgdg-testing > $null
    }
    else {
        sudo add-apt-repository "deb $PgDgArchiveUrl $(lsb_release -cs)-pgdg-archive main"
        sudo add-apt-repository "deb $PgDgUrl $(lsb_release -cs)-pgdg main"
    }
    sudo apt-get update -qq
    sudo apt-get install -qq postgresql-$Version
    $Env:PGDATA = sudo -u postgres psql -c "SHOW data_directory" -Atq
}

function Install-PostgresWindowsEdb {
    Param(
        [Parameter(Mandatory=$True, Position=0)]
        [string]$Version,
        [Parameter(Mandatory=$True, Position=1)]
        [bool]$IsPrerelease
    )

    if ($IsPrerelease) {
        throw "Installation of PostgreSQL prerelease versions currently is not supported on Windows."
    }
    $edbVersion = ([xml](Invoke-WebRequest -Uri $ApplicationsXmlUrl).content).DocumentElement.SelectNodes("//application[id='postgresql_$($Version -replace '\.','')' and platform='windows-x64']/version/text()").Value
    $downloadUrl = $DownloadUrlTemplate -f $edbVersion
    curl -o pgsql.zip -L $downloadUrl
    unzip pgsql.zip -x 'pgsql/include/**' 'pgsql/doc/**' 'pgsql/pgAdmin 4/**' 'pgsql/StackBuilder/**'
    $Env:PATH = "$(Get-Location)\pgsql\bin;" + $Env:PATH
    $Env:PGDATA = "pgsql/PGDATA"
    initdb -D $Env:PGDATA -E UTF8 -U postgres
}

function Install-PostgresMacOsHomebrew {
    Param(
        [Parameter(Mandatory=$True, Position=0)]
        [string]$Version,
        [Parameter(Mandatory=$True, Position=1)]
        [bool]$IsPrerelease
    )

    if ($IsPrerelease) {
        throw "Installation of PostgreSQL prerelease versions currently is not supported on MacOs."
    }
    brew update
    brew uninstall --force postgresql
    rm -rf $Env:PGDATA
    brew install postgresql@$Version
    $Env:PATH = "/usr/local/opt/postgresql@$Version/bin:" + $Env:PATH
}

function Install-Postgres {
    Param(
        [Parameter(Mandatory=$True, Position=0)]
        [ValidatePattern("^\d+(?:\.\d+)?$")]
        [string]$Version,
        [switch]$Prerelease
    )

    if ($IsLinux) {
        Install-PostgresUbuntuPgDg $Version $Prerelease
    }
    elseif ($IsWindows) {
        Install-PostgresWindowsEdb $Version $Prerelease
    }
    elseif ($IsMacOS) {
        Install-PostgresMacOsHomebrew $Version $Prerelease
    }
}

function Wait-Postgres {
    $loopCount = 1
    do
    {
        $loopCount++
        pg_isready -h localhost -U postgres -d postgres -q
    } while ((-not $?) -and ($loopCount -le 5))
}

function Get-PostgresVersion {
    (psql -V) -replace '^(?:\D*(?<MajorVersion>\d{2})\.\d+\D*)|(?:\D*(?<MajorVersion>\d{1})(?<Minor>\.\d+)\.\d+\D*)$','${MajorVersion}${Minor}'
}

function Start-Postgres {
    Param(
        [Parameter(Mandatory=$False)]
        [AllowNull()]
        [AllowEmptyString()]
        [ValidatePattern("(?:^$)|(?:^\d+(?:\.\d+)?$)")]
        [String]$Version
    )
    if ($IsLinux) {
        if ([string]::IsNullOrEmpty($Version)) {
            $Version = Get-PostgresVersion
        }
        
        sudo pg_ctlcluster $Version main start
    }
    elseif ($IsWindows) {
        pg_ctl -D $Env:PGDATA start
    }
    elseif ($IsMacOS) {
        if ([string]::IsNullOrEmpty($Version)) {
            brew services start postgresql
        }
        else {
            brew services start postgresql@$Version
        }
    }
    Wait-Postgres
}

function Restart-Postgres {
    Param(
        [Parameter(Mandatory=$False)]
        [AllowNull()]
        [AllowEmptyString()]
        [ValidatePattern("(?:^$)|(?:^\d+(?:\.\d+)?$)")]
        [String]$Version
    )
    if ($IsLinux) {
        if ([string]::IsNullOrEmpty($Version)) {
            $Version = Get-PostgresVersion
        }
        
        sudo pg_ctlcluster $Version main restart
    }
    elseif ($IsWindows) {
        pg_ctl -D $Env:PGDATA restart
    }
    elseif ($IsMacOS) {
        if ([string]::IsNullOrEmpty($Version)) {
            brew services restart postgresql
        }
        else {
            brew services restart postgresql@$Version
        }
    }
    Wait-Postgres
}

function Update-PostgresConfiguration {
    Param(
        [Parameter(Mandatory=$False)]
        [AllowNull()]
        [AllowEmptyString()]
        [ValidatePattern("(?:^$)|(?:^\d+(?:\.\d+)?$)")]
        [String]$Version
    )

    if ([string]::IsNullOrEmpty($Version)) {
        $Version = Get-PostgresVersion
    }
    $versionMatch = [System.Text.RegularExpressions.Regex]::Match($Version, "^(?<Major>\d+)(?:\.(?<Minor>\d+))?$")
    $major = [Int32]::Parse($versionMatch.Groups["Major"].Value)
    if ($versionMatch.Groups["Minor"].Success) {
        $minor = [Int32]::Parse($versionMatch.Groups["Minor"].Value)
    }
    else {
        $minor = -1
    }

    $script = [System.Text.StringBuilder]::new()
    $createDefaultUsers = "CREATE USER npgsql_tests SUPERUSER PASSWORD 'npgsql_tests';
        CREATE USER npgsql_tests_ssl SUPERUSER PASSWORD 'npgsql_tests_ssl';
        CREATE USER npgsql_tests_nossl SUPERUSER PASSWORD 'npgsql_tests_nossl';"

    if ($major -ge 10) {
        [void]$script.AppendLine("SET password_encryption = 'md5';")
        [void]$script.AppendLine($createDefaultUsers)
        [void]$script.AppendLine("SET password_encryption = 'scram-sha-256';")
        [void]$script.AppendLine("CREATE USER npgsql_tests_scram SUPERUSER PASSWORD 'npgsql_tests_scram';")
        [void]$script.AppendLine("ALTER SYSTEM SET password_encryption = 'scram-sha-256';")
    }
    else {
        [void]$script.AppendLine("SET password_encryption = 'on';")
        [void]$script.AppendLine($createDefaultUsers)
    }

    [void]$script.AppendLine("ALTER SYSTEM SET ssl = 'on';")
    [void]$script.AppendLine("SHOW data_directory \gset")
    [void]$script.AppendLine("\qecho :data_directory")
    [void]$script.AppendLine("SELECT :'data_directory'  || '/server.crt' AS ssl_cert_file \gset")
    [void]$script.AppendLine("SELECT :'data_directory'  || '/server.key' AS ssl_key_file \gset")
    [void]$script.AppendLine("ALTER SYSTEM SET ssl_cert_file = :'ssl_cert_file';")
    [void]$script.AppendLine("ALTER SYSTEM SET ssl_key_file = :'ssl_key_file';")
    [void]$script.AppendLine("ALTER SYSTEM SET wal_level = 'logical';")
    [void]$script.AppendLine("ALTER SYSTEM SET max_wal_senders = 50;")
    [void]$script.AppendLine("ALTER SYSTEM SET wal_sender_timeout = '3s';")
    [void]$script.AppendLine("ALTER SYSTEM SET synchronous_standby_names = 'npgsql_test_sync_standby';")
    [void]$script.AppendLine("ALTER SYSTEM SET synchronous_commit = 'local';")

    if ($major -ge 13) {
        [void]$script.AppendLine("ALTER SYSTEM SET logical_decoding_work_mem = '64kB';")
    }
    if ($IsLinux -and $major -ge 14) {
        [void]$script.AppendLine("SHOW unix_socket_directories \gset")
        [void]$script.AppendLine("ALTER SYSTEM SET unix_socket_directories = :'unix_socket_directories','@/npgsql_unix';")
    }
    elseif ($IsWindows) {
        [void]$script.AppendLine("ALTER SYSTEM SET unix_socket_directories = '$($Env:TEMP -replace '\\','/')';")
    }
    elseif ($IsMacOS) {
        [void]$script.AppendLine("ALTER SYSTEM SET unix_socket_directories = '/tmp';")
    }

    $script.ToString()
    if ($IsLinux) {
        $script.ToString() | sudo -u postgres psql -Atq-U postgres -d postgres
    }
    else {
        $script.ToString() | psql -Atq -U postgres -d postgres
    }
}

function Update-PostgresHbaFile {
    Param(
        [Parameter(Mandatory=$False)]
        [AllowNull()]
        [AllowEmptyString()]
        [ValidatePattern("(?:^$)|(?:^\d+(?:\.\d+)?$)")]
        [String]$Version
    )

    if ([string]::IsNullOrEmpty($Version)) {
        $Version = Get-PostgresVersion
    }
    $versionMatch = [System.Text.RegularExpressions.Regex]::Match($Version, "^(?<Major>\d+)(?:\.(?<Minor>\d+))?$")
    $major = [Int32]::Parse($versionMatch.Groups["Major"].Value)
    if ($versionMatch.Groups["Minor"].Success) {
        $minor = [Int32]::Parse($versionMatch.Groups["Minor"].Value)
    }
    else {
        $minor = -1
    }

    $hbaContent = [System.Text.StringBuilder]::new()
    [void]$hbaContent.AppendLine("# TYPE      DATABASE      USER                 ADDRESS   METHOD")
    if ((-not $IsWindows) -or $major -ge 13) {
        [void]$hbaContent.AppendLine("local       all           all                            trust")
    }
    if ($IsWindows) {
        [void]$hbaContent.AppendLine("host        postgres      postgres             all       trust")
    }
    if ($major -ge 10) {
        [void]$hbaContent.AppendLine("host        all           npgsql_tests_scram   all       scram-sha-256")
    }
    [void]$hbaContent.AppendLine("hostssl     all           npgsql_tests_ssl     all       md5")
    [void]$hbaContent.AppendLine("hostnossl   all           npgsql_tests_ssl     all       reject")
    [void]$hbaContent.AppendLine("hostnossl   all           npgsql_tests_nossl   all       md5")
    [void]$hbaContent.AppendLine("hostssl     all           npgsql_tests_nossl   all       reject")
    [void]$hbaContent.AppendLine("host        all           all                  all       md5")
    [void]$hbaContent.AppendLine("host        replication   all                  all       md5")

    $hbaContent.ToString()
    if ($IsLinux) {
        $hbaFile = sudo -u postgres psql -c "SHOW hba_file" -Atq -U postgres -d postgres
        $hbaContent.ToString() | sudo tee $hbaFile > $null
    }
    else {
        $hbaFile = psql -c "SHOW hba_file" -Atq -U postgres -d postgres
        if ([string]::IsNullOrEmpty($hbaFile)) {
            $hbaFile = Join-Path -Path $Env:PGDATA -ChildPath 'pg_hba.conf'
        }
        $hbaFile
        $hbaContent.ToString() | Out-File $hbaFile
    }
}

function Copy-SslFiles {
    $buildDir = "$Env:GITHUB_WORKSPACE/.build"

    if ($IsLinux) {
        sudo cp $buildDir/server.crt $buildDir/server.key $Env:PGDATA
        sudo chmod 600 $Env:PGDATA/server.crt
        sudo chmod 600 $Env:PGDATA/server.key
        sudo chown postgres $Env:PGDATA/server.crt
        sudo chown postgres $Env:PGDATA/server.key
    }
    else {
        Copy-Item -Path $buildDir/server.* -Include *.crt,*.key -Destination $Env:PGDATA
    }
    if ($IsMacOS) {
        chmod 600 $Env:PGDATA/server.crt
        chmod 600 $Env:PGDATA/server.key
    }
}

Export-ModuleMember -Function Install-Postgres, Start-Postgres, Restart-Postgres, Update-PostgresConfiguration, Update-PostgresHbaFile, Copy-SslFiles
