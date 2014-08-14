all:

install:
	install -d $(DESTDIR)$(PREFIX)/etc/cron.d
	install -d $(DESTDIR)$(PREFIX)/etc/cron.daily
	install -d $(DESTDIR)$(PREFIX)/etc/cron.hourly
	install -d $(DESTDIR)$(PREFIX)/etc/cron.weekly
	install -d $(DESTDIR)$(PREFIX)/etc/cron.monthly
	install -m 644 etc/hdfs-auto-snapshot.cron.frequent $(DESTDIR)$(PREFIX)/etc/cron.d/hdfs-auto-snapshot
	install -m 755 etc/hdfs-auto-snapshot.cron.hourly   $(DESTDIR)$(PREFIX)/etc/cron.hourly/hdfs-auto-snapshot
	install -m 755 etc/hdfs-auto-snapshot.cron.daily    $(DESTDIR)$(PREFIX)/etc/cron.daily/hdfs-auto-snapshot
	install -m 755 etc/hdfs-auto-snapshot.cron.weekly   $(DESTDIR)$(PREFIX)/etc/cron.weekly/hdfs-auto-snapshot
	install -m 755 etc/hdfs-auto-snapshot.cron.monthly  $(DESTDIR)$(PREFIX)/etc/cron.monthly/hdfs-auto-snapshot
	install -d $(DESTDIR)$(PREFIX)/share/man/man8
	install -m 644 src/hdfs-auto-snapshot.8 $(DESTDIR)$(PREFIX)/share/man/man8/hdfs-auto-snapshot.8
	install -d $(DESTDIR)$(PREFIX)/sbin
	install -m 755 src/hdfs-auto-snapshot.sh $(DESTDIR)$(PREFIX)/sbin/hdfs-auto-snapshot
