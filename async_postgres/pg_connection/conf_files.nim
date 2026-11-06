## The files libpq reads besides certificates: the password file (``.pgpass``)
## and the connection service file (``pg_service.conf``), plus the OS user
## their defaults come from. Both formats follow libpq's rules.
##
## Depends only on `pg_errors` and nimcrypto's `burnMem`. Internal module: not
## part of the public API. Import the `pg_connection` hub instead.

import std/strutils
when defined(posix):
  import std/posix
elif defined(windows):
  import std/[oserrors, winlean]

  proc getFileType(
    h: Handle
  ): int32 {.stdcall, dynlib: "kernel32", importc: "GetFileType".}

  proc getUserNameW(
    buf: WideCString, size: ptr int32
  ): WINBOOL {.stdcall, dynlib: "advapi32", importc: "GetUserNameW".}

  const
    FileTypeUnknown = 0'i32
    FileTypeDisk = 1'i32

from pkg/nimcrypto/utils import burnMem

import ../pg_errors

const MaxConfFileBytes* = 4 * 1024 * 1024
  ## Cap for every configuration file read (DoS bound; cert chains fit
  ## comfortably under this).

type ConfFileFault* = enum
  ## Why `readConfFile` read nothing.
  cffNone ## Read.
  cffOpen ## Could not be opened (missing, no access).
  cffStat
  cffNotRegular ## A directory, FIFO, device or the like.
  cffLoosePerms ## Group or world permission bits (POSIX, when checked).
  cffTooLarge ## Over `MaxConfFileBytes`.
  cffRead

proc c_ferror(f: File): cint {.importc: "ferror", header: "<stdio.h>".}
  ## `readBuffer` shorts on both EOF and read error; only this tells them apart.

proc wipe*(s: var string) =
  ## Zero `s` before it is dropped: a file's other lines (passwords, a key)
  ## need not linger in freed memory. `burnMem`'s volatile memset is not
  ## elided as a dead store, which a plain loop before the free may be.
  if s.len > 0:
    burnMem(addr s[0], s.len)

proc readCapped(f: File, sizeHint: int64): string {.raises: [IOError].} =
  ## Read at most `MaxConfFileBytes + 1` bytes: a regular file may report
  ## `st_size == 0` and still stream content (procfs/sysfs), so `readAll`
  ## would grow unbounded. The extra byte makes oversize detectable.
  ##
  ## Grows from `sizeHint` rather than starting at the cap: `setLen` shrinks
  ## the length but not the payload, which would stay resident.
  const limit = MaxConfFileBytes + 1
  var cap =
    if sizeHint <= 0:
      4096
    else:
      int(min(sizeHint + 1, int64(limit)))
  result = newString(cap)
  var total = 0
  while true:
    if total == cap:
      if cap == limit:
        break
      cap = min(cap * 2, limit)
      # Not `setLen`, which would free the old payload unwiped.
      var grown = newString(cap)
      copyMem(addr grown[0], addr result[0], total)
      wipe(result)
      result = move grown
    let n = readBuffer(f, addr result[total], cap - total)
    if n <= 0:
      if c_ferror(f) != 0:
        wipe(result)
        # Truncating would hand the caller a short trust store.
        raise newException(IOError, "read failed")
      break
    total += n
  result.setLen(total)

proc readConfFile*(
    path: string, content: var string, checkPerms = false
): ConfFileFault {.raises: [].} =
  ## Read the regular file at `path` into `content`. With `checkPerms`, any
  ## group or world permission bit is a fault (POSIX only: libpq skips the
  ## check on Windows too). `path` must hold no NUL; the C `open` would cut it.
  content = ""
  when defined(posix):
    # O_NONBLOCK so a FIFO with no writer cannot block the open, then fstat
    # the same fd: a separate stat()+open() would leave a TOCTOU window in an
    # attacker-writable directory.
    let fd = posix.open(path, O_RDONLY or O_NONBLOCK or O_CLOEXEC)
    if fd < 0:
      return cffOpen
    var st: Stat
    if fstat(fd, st) != 0:
      discard close(fd)
      return cffStat
    if not S_ISREG(st.st_mode):
      discard close(fd)
      return cffNotRegular
    if checkPerms and (st.st_mode.int and (S_IRWXG or S_IRWXO).int) != 0:
      discard close(fd)
      return cffLoosePerms
    if st.st_size > Off(MaxConfFileBytes):
      discard close(fd)
      return cffTooLarge
    let flags = fcntl(fd, F_GETFL)
    if flags == -1 or fcntl(fd, F_SETFL, flags and not O_NONBLOCK) == -1:
      discard close(fd)
      return cffOpen
    var f: File
    if not open(f, fd, fmRead):
      discard close(fd)
      return cffOpen
    try:
      content = readCapped(f, int64(st.st_size))
    except IOError:
      content = ""
      return cffRead
    finally:
      close(f)
  else:
    discard checkPerms
    var f: File
    if not open(f, path, fmRead):
      return cffOpen
    try:
      when defined(windows):
        # Check the opened handle: a device name (NUL, CON) or a pipe opens
        # without blocking — only reading it does — and reparse-point files
        # (OneDrive placeholders) stay DISK.
        let fileType = getFileType(Handle(getOsFileHandle(f)))
        if fileType != FileTypeDisk:
          # UNKNOWN with an error set is a failed call, not a file kind.
          if fileType == FileTypeUnknown and osLastError() != OSErrorCode(0):
            return cffRead
          return cffNotRegular
      let size = getFileSize(f)
      if size > MaxConfFileBytes:
        return cffTooLarge
      content = readCapped(f, size)
    except IOError:
      content = ""
      return cffRead
    finally:
      close(f)
  if content.len > MaxConfFileBytes:
    # Growth after the size check (TOCTOU).
    wipe(content)
    content = ""
    return cffTooLarge
  cffNone

proc confFileMissing*(path: string): bool {.raises: [].} =
  ## Whether nothing is at `path`, which libpq's `stat` reports as ENOENT or
  ## ENOTDIR, rather than a file that cannot be read.
  if '\0' in path:
    return false # the caller's read rejects it
  when defined(posix):
    var st: Stat
    stat(path.cstring, st) != 0 and (errno == ENOENT or errno == ENOTDIR)
  elif defined(windows):
    if getFileAttributesW(newWideCString(path)) != -1'i32:
      return false
    let err = osLastError().int32
    err == ERROR_FILE_NOT_FOUND or err == ERROR_PATH_NOT_FOUND
  else:
    false

proc confFileStats*(path: string): bool {.raises: [].} =
  ## Whether `stat` succeeds on `path`: libpq skips a service file, or an
  ## sslrootcert it need not verify with, that it cannot stat for any reason.
  if '\0' in path:
    return true # the caller's read rejects it
  when defined(posix):
    var st: Stat
    stat(path.cstring, st) == 0
  elif defined(windows):
    getFileAttributesW(newWideCString(path)) != -1'i32
  else:
    true

# Password file

type PassFileLookup* = object ## What `lookupPassFile` found for one host.
  password*: string
  found*: bool
  note*: string
    ## Added to a 28P01 refusal: where the password came from, or why the
    ## file was skipped. Empty: nothing worth saying.

proc matchPassField(content: string, pos: var int, stop: int, token: string): bool =
  ## Whether the field at `pos` (in a line ending at `stop`) matches `token`,
  ## as libpq does: a whole-field ``*`` matches anything, ``\`` takes the next
  ## character literally, and `pos` moves past the field's ``:``.
  if pos + 1 < stop and content[pos] == '*' and content[pos + 1] == ':':
    pos += 2
    return true
  var i = pos
  var t = 0
  while i < stop:
    var escaped = false
    if content[i] == '\\':
      inc i
      escaped = true
      if i >= stop:
        return false
    if content[i] == ':' and t == token.len and not escaped:
      pos = i + 1
      return true
    if t == token.len or content[i] != token[t]:
      return false
    inc i
    inc t
  false

proc passFieldValue(content: string, pos, stop: int): string =
  ## The password from `pos` to `stop`, de-escaped; it ends at an unescaped
  ## ``:``. Sized up front: a regrown buffer would free a prefix unwiped.
  result = newStringOfCap(stop - pos)
  var i = pos
  while i < stop and content[i] != ':':
    if content[i] == '\\' and i + 1 < stop:
      inc i
    result.add content[i]
    inc i

proc findPassword*(
    content, host, port, database, user: string
): tuple[found: bool, password: string] =
  ## The first line of password file `content` matching all four keys.
  ## Scanned in place: only the password returned is copied out of `content`.
  var start = 0
  while start < content.len:
    var next = content.find('\n', start)
    if next < 0:
      next = content.len
    var stop = next
    while stop > start and content[stop - 1] == '\r':
      dec stop
    # A C reader would cut the line at a NUL; skip it rather than match a
    # prefix or hand back a password that cannot be sent.
    if stop > start and content[start] != '#' and content.find('\0', start, stop - 1) < 0:
      var pos = start
      if matchPassField(content, pos, stop, host) and
          matchPassField(content, pos, stop, port) and
          matchPassField(content, pos, stop, database) and
          matchPassField(content, pos, stop, user):
        return (true, passFieldValue(content, pos, stop))
    start = next + 1

proc lookupPassFile*(
    path, host, port, database, user: string
): PassFileLookup {.raises: [].} =
  ## The password for one host from the password file at `path`, as libpq
  ## does. A file that is missing, unreadable or unsafe is skipped, never an
  ## error. `host` is the matching key (``localhost`` for a Unix socket),
  ## `database` already defaulted to `user`.
  if path.len == 0 or '\0' in path or database.len == 0 or user.len == 0:
    return
  var content: string
  let fault = readConfFile(path, content, checkPerms = true)
  case fault
  of cffNone:
    discard
  of cffNotRegular:
    result.note = "password file \"" & path & "\" is not a plain file"
    return
  of cffLoosePerms:
    result.note =
      "password file \"" & path &
      "\" has group or world access; permissions should be u=rw (0600) or less"
    return
  of cffTooLarge:
    result.note =
      "password file \"" & path & "\" exceeds the size limit (" & $MaxConfFileBytes &
      " bytes)"
    return
  of cffOpen, cffStat, cffRead:
    return
  let got = findPassword(content, host, port, database, user)
  wipe(content) # libpq zeroes its buffer too
  if got.found:
    result.found = true
    result.password = got.password
    result.note = "password retrieved from file \"" & path & "\""

# Service file

type ServiceEntry* = object ## One ``key=value`` line of a service section.
  key*: string
  value*: string
  line*: int

proc parseServiceSection*(
    content, path, service: string, found: var bool
): seq[ServiceEntry] {.raises: [PgConfigError].} =
  ## The entries of section `service` in service file `content`, as libpq:
  ## the first section of that name wins, and within it the first value of a
  ## key. Unlike libpq, whitespace around ``=`` is trimmed, and lines have no
  ## length limit. Only the section's own lines are checked. Errors name the
  ## file and line, never a value.
  ##
  ## Scanned in place, as `findPassword`: other sections' passwords are never
  ## copied out of `content`, so wiping it is enough.
  found = false
  var lineNo = 0
  var start = 0
  while start < content.len:
    inc lineNo
    var next = content.find('\n', start)
    if next < 0:
      next = content.len
    var first = start # the line's [first, stop), whitespace-trimmed
    var stop = next
    start = next + 1
    while first < stop and content[first] in Whitespace:
      inc first
    while stop > first and content[stop - 1] in Whitespace:
      dec stop
    if first == stop or content[first] == '#':
      continue
    if content[first] == '[':
      if found:
        break
      found =
        stop - first > service.len + 1 and content.continuesWith(service, first + 1) and
        content[first + service.len + 1] == ']'
      continue
    if not found:
      continue
    let where = "service file \"" & path & "\", line " & $lineNo
    if content.continuesWith("ldap", first):
      raise newException(PgConfigError, "LDAP lookup is not supported in " & where)
    let eq = content.find('=', first, stop - 1)
    if eq < 0:
      raise newException(PgConfigError, "syntax error in " & where)
    var keyStop = eq
    while keyStop > first and content[keyStop - 1] in Whitespace:
      dec keyStop
    if keyStop == first:
      raise newException(PgConfigError, "syntax error in " & where)
    let key = content[first ..< keyStop]
    if key == "service":
      raise newException(
        PgConfigError, "nested service specifications not supported in " & where
      )
    if content.find('\0', first, stop - 1) >= 0:
      raise newException(PgConfigError, "Forbidden zero byte in " & where)
    var dup = false
    for e in result:
      if e.key == key:
        dup = true
        break
    if not dup:
      var valueStart = eq + 1
      while valueStart < stop and content[valueStart] in Whitespace:
        inc valueStart
      result.add ServiceEntry(
        key: key, value: content[valueStart ..< stop], line: lineNo
      )

# OS user

proc osUserInfo*(): tuple[name, home: string] {.raises: [].} =
  ## The effective user's name and, on POSIX, home directory from the user
  ## database, the fallback libpq uses. Empty when unknown.
  when defined(posix):
    var pwd: Passwd
    var res: ptr Passwd
    var buf = newString(16384)
    if getpwuid_r(geteuid(), addr pwd, cstring(buf), buf.len, addr res) == 0 and
        res != nil:
      if pwd.pw_name != nil:
        result.name = $pwd.pw_name
      if pwd.pw_dir != nil:
        result.home = $pwd.pw_dir
  elif defined(windows):
    const unlen = 256 # UNLEN
    var buf = newWideCString(unlen + 1)
    var size = int32(unlen + 1)
    if getUserNameW(buf, addr size) != 0:
      result.name = $buf
