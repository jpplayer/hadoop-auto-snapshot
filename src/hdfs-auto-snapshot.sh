#!/bin/sh

# hdfs-auto-snapshot for Linux
# Automatically create, rotate, and destroy periodic hdfs snapshots.
# Copyright 2011 Darik Horn <dajhorn@vanadac.com>
# Copyright 2014 Jean-Philippe Player <jpplayer@gmail.com>
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA  02111-1307  USA
#

# Set the field separator to a literal tab and newline.
IFS="	
"

# Set default program options.
opt_backup_full=''
opt_backup_incremental=''
opt_default_exclude=''
opt_dry_run=''
opt_event='-'
opt_fast_hdfs_list=''
opt_keep=''
opt_label=''
opt_prefix='hdfs-auto-snap'
opt_recursive=''
opt_sep='_'
opt_setauto=''
opt_syslog=''
opt_skip_scrub=''
opt_verbose=''

# Global summary statistics.
DESTRUCTION_COUNT='0'
SNAPSHOT_COUNT='0'
WARNING_COUNT='0'

# Other global variables.
SNAPSHOTS_OLD=''


print_usage ()
{
	echo "Usage: $0 [options] [-l label] <'//' | name [name...]>
  --default-exclude  Exclude datasets 
  -d, --debug        Print debugging messages.
  -e, --event=EVENT  Set the com.sun:auto-snapshot-desc property to EVENT.
      --fast         Use a faster hdfs list invocation.
  -n, --dry-run      Print actions without actually doing anything.
  -h, --help         Print this usage message.
  -k, --keep=NUM     Keep NUM recent snapshots and destroy older snapshots.
  -l, --label=LAB    LAB is usually 'hourly', 'daily', or 'monthly'.
  -p, --prefix=PRE   PRE is 'hdfs-auto-snap' by default.
  -q, --quiet        Suppress warnings and notices at the console.
      --sep=CHAR     Use CHAR to separate date stamps in snapshot names.
  -g, --syslog       Write messages into the system log.
  -v, --verbose      Print info messages.
      name           Filesystem and volume names, or '//' for all hdfs datasets.
" 
}


print_log () # level, message, ...
{
	LEVEL=$1
	shift 1

	case $LEVEL in
		(eme*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.emerge $*
			echo Emergency: $* 1>&2
			;;
		(ale*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.alert $*
			echo Alert: $* 1>&2
			;;
		(cri*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.crit $*
			echo Critical: $* 1>&2
			;;
		(err*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.err $*
			echo Error: $* 1>&2
			;;
		(war*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.warning $*
			test -z "$opt_quiet" && echo Warning: $* 1>&2
			;;
		(not*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.notice $*
			test -z "$opt_quiet" && echo $*
			;;
		(inf*)
			# test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.info $*
			test -n "$opt_verbose" && echo $*
			;;
		(deb*)
			# test -n "$opt_syslog" && logger -t "$opt_prefix" -p daemon.debug $*
			test -n "$opt_debug" && echo Debug: $*
			;;
		(*)
			test -n "$opt_syslog" && logger -t "$opt_prefix" $*
			echo $* 1>&2
			;;
	esac
}


do_run () # [argv]
{
	if [ -n "$opt_dry_run" ]
	then
		echo $*
		RC="$?"
	else
		eval $*
		RC="$?"
		if [ "$RC" -eq '0' ]
		then
			print_log debug "$*"
		else
			print_log warning "$* returned $RC"
		fi
	fi
	return "$RC"
}

#target = dir
do_snapshots () # properties, flags, snapname, oldglob, [targets...]
{
	local PROPS="$1"
	local FLAGS="$2"
	local NAME="$3"
	local GLOB="$4"
	local TARGETS="$5"
	local KEEP=''

	# global DESTRUCTION_COUNT
	# global SNAPSHOT_COUNT
	# global WARNING_COUNT
	# global SNAPSHOTS_OLD

        # For each hdfs path
	for ii in $TARGETS
	do
		#if do_run "hdfs snapshot $PROPS $FLAGS '$ii@$NAME'"
		test -n "$opt_debug" && echo Debug: processing path $ii
		if do_run "hdfs dfs -createSnapshot '$ii' '$NAME'"
		then
			SNAPSHOT_COUNT=$(( $SNAPSHOT_COUNT + 1 ))
		else
			WARNING_COUNT=$(( $WARNING_COUNT + 1 ))
			continue
		fi 

		# Retain at most $opt_keep number of old snapshots of this filesystem,
		# including the one that was just recently created.
		test -z "$opt_keep" && continue
		KEEP="$opt_keep"

		# ASSERT: The old snapshot list is sorted by increasing age.
		for jj in $SNAPSHOTS_OLD
		do
			#test -n "$opt_debug" && echo Debug: processing old snapshot $jj
			# Check whether this is an old snapshot of the filesystem.
			#if [ -z "${jj#$ii@$GLOB}" ]
			if [ -z "${jj#$ii.snapshot/$GLOB}" ]
			then
				test -n "$opt_debug" && echo Debug: checking for old snapshot $jj
				KEEP=$(( $KEEP - 1 ))
				if [ "$KEEP" -le '0' ]
				then
					OLD=$( echo "$jj" | sed 's;.*\.snapshot/;;' )
					if do_run "hdfs dfs -deleteSnapshot '$ii' '$OLD'" 
					then
						DESTRUCTION_COUNT=$(( $DESTRUCTION_COUNT + 1 ))
					else
						WARNING_COUNT=$(( $WARNING_COUNT + 1 ))
					fi
				fi
			fi
		done
	done
}


# main ()
# {

GETOPT=$(getopt \
  --longoptions=default-exclude,dry-run,fast,skip-scrub,recursive \
  --longoptions=event:,keep:,label:,prefix:,sep: \
  --longoptions=debug,help,quiet,syslog,verbose \
  --options=dnshe:l:k:p:rs:qgv \
  -- "$@" ) \
  || exit 128

eval set -- "$GETOPT"

while [ "$#" -gt '0' ]
do
	case "$1" in
		(-d|--debug)
			opt_debug='1'
			opt_quiet=''
			opt_verbose='1'
			shift 1
			;;
		(--default-exclude)
			opt_default_exclude='1'
			shift 1
			;;
		(-e|--event)
			if [ "${#2}" -gt '1024' ]
			then
				print_log error "The $1 parameter must be less than 1025 characters."
				exit 139
			elif [ "${#2}" -gt '0' ]
			then
				opt_event="$2"
			fi
			shift 2
			;;
		(--fast)
			opt_fast_hdfs_list='1'
			shift 1
			;;
		(-n|--dry-run)
			opt_dry_run='1'
			shift 1
			;;
		(-s|--skip-scrub)
			opt_skip_scrub='1'
			shift 1
			;;
		(-h|--help)
			print_usage
			exit 0
			;;
		(-k|--keep)
			if ! test "$2" -gt '0' 2>/dev/null
			then
				print_log error "The $1 parameter must be a positive integer."
				exit 129
			fi
			opt_keep="$2"
			shift 2
			;;
		(-l|--label)
			opt_label="$2"
			shift 2
			;;
		(-p|--prefix)
			opt_prefix="$2"
			while test "${#opt_prefix}" -gt '0'
			do
				case $opt_prefix in
					([![:alnum:]_.:\ -]*)
						print_log error "The $1 parameter must be alphanumeric."
						exit 130
						;;
				esac
				opt_prefix="${opt_prefix#?}"
			done
			opt_prefix="$2"
			shift 2
			;;
		(-q|--quiet)
			opt_debug=''
			opt_quiet='1'
			opt_verbose=''
			shift 1
			;;
		(-r|--recursive)
			opt_recursive='1'
			shift 1
			;;
		(--sep)
			case "$2" in 
				([[:alnum:]_.:\ -])
					:
					;;
				('')
					print_log error "The $1 parameter must be non-empty."
					exit 131
					;;
				(*)
					print_log error "The $1 parameter must be one alphanumeric character."
					exit 132
					;;
			esac
			opt_sep="$2"
			shift 2
			;;
		(-g|--syslog)
			opt_syslog='1'
			shift 1
			;;
		(-v|--verbose)
			opt_quiet=''
			opt_verbose='1'
			shift 1
			;;
		(--)
			shift 1
			break
			;;
	esac
done

if [ "$#" -eq '0' ]
then
	print_log error "The filesystem argument list is empty."
	exit 133
fi 

# Count the number of times '//' appears on the command line.
SLASHIES='0'
for ii in "$@"
do
	test "$ii" = '//' && SLASHIES=$(( $SLASHIES + 1 ))
done

if [ "$#" -gt '1' -a "$SLASHIES" -gt '0' ]
then
	print_log error "The // must be the only argument if it is given."
	exit 134
fi

# Ensure the path ends with a slash
HDFS_LIST=$(hdfs lsSnapshottableDir  | cut -f 10 -d ' ' | sed 's;[^/]$;\0/;' ) \
  || { print_log error "hdfs lsSnapshottableDir  | cut -f 10 -d ' ' | sed 's;[^/]$;\0/;' $?: $HDFS_LIST"; exit 136; }


# For each Snapshottable Directory get list of snapshots. This can be large.
if [ -n "$opt_fast_hdfs_list" ]
then
        SNAPSHOTS_OLD=$(hdfs list -H -t snapshot -o name -s name|grep $opt_prefix |awk '{ print substr( $0, length($0) - 14, length($0) ) " " $0}' |sort -r -k1,1 -k2,2|awk '{ print substr( $0, 17, length($0) )}') \
          || { print_log error "hdfs list $?: $SNAPSHOTS_OLD"; exit 137; }
else

        # Assert: snpashots listed with newest first
        SNAPSHOTS_OLD="";
        for path in $HDFS_LIST; do

echo processing $path
        SNAPSHOTS_OLD_DIR=$(hdfs dfs -ls "$path.snapshot" | sort -rk6,6 -rk7,7 | awk '{ print $8 }' ) \
          || { print_log error "hdfs dfs -ls "$path.snapshot" | sort -rk6,6 -rk7,7 | awk '{ print $8 }'  $?: $SNAPSHOTS_OLD_DIR"; exit 137; }
	test -n "$opt_debug" && echo Snapshots old dir is $SNAPSHOTS_OLD_DIR. snapshots old is $SNAPSHOTS_OLD.
	SNAPSHOTS_OLD="$SNAPSHOTS_OLD"$'\n'"$SNAPSHOTS_OLD_DIR"
        done

	test -n "$opt_debug" && echo Debug: old snapshots are  "$SNAPSHOTS_OLD"
fi

# Verify that each argument is an HDFS path.
for ii in "$@"
do
	test "$ii" = '//' && continue 1
	while read NAME PROPERTIES
	do
		test "$ii" = "$NAME" && continue 2
	done <<-HERE
	$HDFS_LIST
	HERE
	print_log error "$ii is not an hdfs path."
	exit 138
done

# If the --default-exclude flag is set, then exclude all datasets that lack
# an explicit com.sun:auto-snapshot* property. Otherwise, include them.
#if [ -n "$opt_default_exclude" ]
#then
	# Get a list of datasets for which snapshots are explicitly enabled.
#	CANDIDATES=$(echo "$HDFS_LIST" | awk -F '\t' \
#	  'tolower($2) ~ /true/ || tolower($3) ~ /true/ {print $1}')
#else
	# Invert the NOAUTO list.
#	CANDIDATES=$(echo "$hdfs_LIST" | awk -F '\t' \
#	  'tolower($2) !~ /false/ && tolower($3) !~ /false/ {print $1}')
#fi

CANDIDATES="$HDFS_LIST"

# Initialize the list of datasets that will get a recursive snapshot.
TARGETS_RECURSIVE=''

# Initialize the list of datasets that will get a non-recursive snapshot.
TARGETS_REGULAR=''

for ii in $CANDIDATES
do
	# Qualify dataset names so variable globbing works properly.
	# Suppose ii=tanker/foo and jj=tank sometime during the loop.
	# Just testing "$ii" != ${ii#$jj} would incorrectly match.
	iii="$ii/"

	# Exclude datasets that are not named on the command line.
	IN_ARGS='0'
	for jj in "$@"
	do
		if [ "$jj" = '//' -o "$jj" = "$ii" ]
		then
			IN_ARGS=$(( $IN_ARGS + 1 ))
		fi
	done
	if [ "$IN_ARGS" -eq '0' ]
	then
		continue
	fi

	for jj in $NOAUTO
	do
		# Ibid regarding iii.
		jjj="$jj/"

		# The --recursive switch only matters for non-wild arguments.
		if [ -z "$opt_recursive" -a "$1" != '//' ]
		then
			# Snapshot this dataset non-recursively.
			print_log debug "Including $ii for regular snapshot."
			TARGETS_REGULAR="${TARGETS_REGULAR:+$TARGETS_REGULAR	}$ii" # nb: \t
			continue 2
		# Check whether the candidate name is a prefix of any excluded dataset name.
		elif [ "$jjj" != "${jjj#$iii}" ]
		then
			# Snapshot this dataset non-recursively.
			print_log debug "Including $ii for regular snapshot."
			TARGETS_REGULAR="${TARGETS_REGULAR:+$TARGETS_REGULAR	}$ii" # nb: \t
			continue 2
		fi
	done

	for jj in $TARGETS_RECURSIVE
	do
		# Ibid regarding iii.
		jjj="$jj/"

		# Check whether any included dataset is a prefix of the candidate name.
		if [ "$iii" != "${iii#$jjj}" ]
		then
			print_log debug "Excluding $ii because $jj includes it recursively."
			continue 2
		fi
	done

	# Append this candidate to the recursive snapshot list because it:
	#
	#   * Does not have an exclusionary property.
	#   * Is in a pool that can currently do snapshots.
	#   * Does not have an excluded descendent filesystem.
	#   * Is not the descendant of an already included filesystem.
	#
	print_log debug "Including $ii for recursive snapshot."
	TARGETS_RECURSIVE="${TARGETS_RECURSIVE:+$TARGETS_RECURSIVE	}$ii" # nb: \t
done

# ISO style date; fifteen characters: YYYY-MM-DD-HHMM
# On Solaris %H%M expands to 12h34.
DATE=$(date --utc +%F-%H%M)

# The snapshot name after the .snapshot path.
SNAPNAME="$opt_prefix${opt_label:+$opt_sep$opt_label}-$DATE"

# The expression for matching old snapshots.  -YYYY-MM-DD-HHMM
SNAPGLOB="$opt_prefix${opt_label:+?$opt_label}????????????????"

test -n "$TARGETS_REGULAR" \
  && print_log info "Doing regular snapshots of $TARGETS_REGULAR"

test -n "$TARGETS_RECURSIVE" \
  && print_log info "Doing recursive snapshots of $TARGETS_RECURSIVE"

test -n "$opt_dry_run" \
  && print_log info "Doing a dry run. Not running these commands..."

do_snapshots "$SNAPPROP" ""   "$SNAPNAME" "$SNAPGLOB" "$TARGETS_REGULAR"
do_snapshots "$SNAPPROP" "-r" "$SNAPNAME" "$SNAPGLOB" "$TARGETS_RECURSIVE"

print_log notice "@$SNAPNAME," \
  "$SNAPSHOT_COUNT created," \
  "$DESTRUCTION_COUNT destroyed," \
  "$WARNING_COUNT warnings."

exit 0
# }
