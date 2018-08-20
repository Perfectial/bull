/*eslint-env node */
'use strict';

var _ = require('lodash');
var parser = require('cron-parser');
var crypto = require('crypto');

var Job = require('./job');

module.exports = function(Queue) {
  Queue.prototype.nextRepeatableJob = function(name, data, opts) {
    var _this = this;
    var client = this.client;
    var repeat = opts.repeat;
    var prevMillis = opts.prevMillis || 0;

    if (!prevMillis && opts.jobId) {
      repeat.jobId = opts.jobId;
    }

    repeat.count = repeat.count ? repeat.count + 1 : 1;

    if (!_.isUndefined(repeat.limit) && repeat.count > repeat.limit) {
      return Promise.resolve();
    }

    var now = Date.now();
    now = prevMillis < now ? now : prevMillis;

    var nextMillis = getNextMillis(now, repeat.cron, repeat);
    if (nextMillis) {
      var jobId = repeat.jobId ? repeat.jobId + ':' : ':';
      var repeatJobKey = getRepeatKey(name, repeat, jobId);

      nextMillis = nextMillis.getTime();

      return client
        .zadd(_this.keys.repeat, nextMillis, repeatJobKey)
        .then(function() {
          //
          // Generate unique job id for this iteration.
          //
          var customId = getRepeatJobId(
            name,
            jobId,
            nextMillis,
            md5(repeatJobKey)
          );
          now = Date.now();
          var delay = nextMillis - now;

          return Job.create(
            _this,
            name,
            data,
            _.extend(_.clone(opts), {
              jobId: customId,
              delay: delay < 0 ? 0 : delay,
              timestamp: now,
              prevMillis: nextMillis
            })
          );
        });
    } else {
      return Promise.resolve();
    }
  };

  Queue.prototype.removeRepeatable = function(name, repeat) {
    var _this = this;

    if (typeof name !== 'string') {
      repeat = name;
      name = Job.DEFAULT_JOB_NAME;
    }

    return this.isReady().then(function() {
      var jobId = repeat.jobId ? repeat.jobId + ':' : ':';
      var repeatJobKey = getRepeatKey(name, repeat, jobId);
      var repeatJobId = getRepeatJobId(name, jobId, '', md5(repeatJobKey));
      var queueKey = _this.keys[''];
      return _this.client.removeRepeatable(
        _this.keys.repeat,
        _this.keys.delayed,
        repeatJobId,
        repeatJobKey,
        queueKey
      );
    });
  };

  Queue.prototype.getRepeatableJobs = function(start, end, asc) {
    var key = this.keys.repeat;
    start = start || 0;
    end = end || -1;
    return (asc
      ? this.client.zrange(key, start, end, 'WITHSCORES')
      : this.client.zrevrange(key, start, end, 'WITHSCORES')
    ).then(function(result) {
      var jobs = [];
      for (var i = 0; i < result.length; i += 2) {
        var data = result[i].split(':');
        jobs.push({
          key: result[i],
          name: data[0],
          id: data[1] || null,
          endDate: parseInt(data[2]) || null,
          tz: data[3] || null,
          cron: data[4],
          next: parseInt(result[i + 1])
        });
      }
      return jobs;
    });
  };

  Queue.prototype.getRepeatableCount = function() {
    return this.client.zcard(this.toKey('repeat'));
  };

  function getRepeatJobId(name, jobId, nextMillis, namespace) {
    return 'repeat:' + md5(name + jobId + namespace) + ':' + nextMillis;
  }

  function getRepeatKey(name, repeat, jobId) {
    var endDate = repeat.endDate
      ? new Date(repeat.endDate).getTime() + ':'
      : ':';
    var tz = repeat.tz ? repeat.tz + ':' : ':';
    return name + ':' + jobId + endDate + tz + repeat.cron;
  }

  function getNextInterval(currentDate, cron, opts) {
    var interval = parser.parseExpression(
      cron,
      _.defaults(
        {
          currentDate: currentDate
        },
        opts
      )
    );

    try {
      return interval.next();
    } catch (e) {
      // Ignore error
    }
  }

  function getNextMillis(millis, cron, opts) {
    var currentDate = new Date(millis);

    var cronTmp = cron;

    if (cron.toLowerCase().includes('l')) {
      var daysInCurrentMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0).getDate();
      var cronTmp = cron.slice(0).replace(/l/gi, daysInCurrentMonth); // cron for last date in current month

      var lastDateInCurrentMonth = getNextInterval(new Date(millis).setDate(daysInCurrentMonth - 1), cronTmp, opts);

      if (lastDateInCurrentMonth.getTime() < currentDate.getTime()) {
        var daysInNextMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 2, 0).getDate();

        cronTmp = cron.replace(/l/gi, daysInNextMonth); // cron for last date in next month
      }
    }

    return getNextInterval(currentDate, cronTmp, opts);
  }

  function md5(str) {
    return crypto
      .createHash('md5')
      .update(str)
      .digest('hex');
  }
};
