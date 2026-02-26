<?php

namespace App;

use function array_fill;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftok;
use function ftell;
use function fwrite;
use function gc_disable;
use function implode;
use function intdiv;
use function ksort;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;
use const SEEK_SET;

final class Parser
{
    private const int WORKERS = 6;
    private const int BUFFER_SIZE = 2_097_152;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $bounds = [0];
        $chunkSize = intdiv($fileSize, self::WORKERS);

        $fh = fopen($inputPath, 'rb');

        stream_set_read_buffer($fh, 0);

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, $i * $chunkSize, SEEK_SET);
            fgets($fh);
            $bounds[] = ftell($fh);
        }
        $bounds[] = $fileSize;

        fseek($fh, 0);

        $chunk = fread($fh, 8_388_608);

        $lastNl = strrpos($chunk, "\n");
        $paths = [];
        $pathCount = 0;
        $dates = [];
        $dateCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nl = strpos($chunk, "\n", $pos + 55);
            $path = substr($chunk, $pos + 25, $nl - $pos - 51);
            $date = substr($chunk, $nl - 25, 10);

            if (!isset($paths[$path])) $paths[$path] = $pathCount++;
            if (!isset($dates[$date])) $dates[$date] = $dateCount++;

            $pos = $nl + 1;
        }

        ksort($dates);

        $zoneSize = $pathCount * $dateCount;
        $zoneSizeBytes = $zoneSize * 4;
        $totalBytes = (self::WORKERS - 1) * $zoneSizeBytes;

        if ($stale = @shmop_open(ftok(__FILE__, 'p'), 'a', 0, 0)) {
            shmop_delete($stale);
        }

        $shm = shmop_open(ftok(__FILE__, 'p'), 'n', 0644, $totalBytes);

        $pids = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                shmop_write($shm, pack('V*', ...self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $pathCount, $dateCount)), $w * $zoneSizeBytes);
                exit(0);
            }
            $pids[] = $pid;
        }

        $merged = self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $pathCount, $dateCount);

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }


        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $rawData = shmop_read($shm, $w * $zoneSizeBytes, $zoneSizeBytes);
            $wCounts = unpack('V*', $rawData);
            $j = 0;
            foreach ($wCounts as $v) {
                $merged[$j++] += $v;
            }
        }

        shmop_delete($shm);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, static::BUFFER_SIZE);
        fwrite($out, '{');

        $offset = 0;
        foreach ($paths as $path => $_) {
            $buf = [];
            $pathBuf=($offset ? ',' : '') . "\n    \"\/blog\/{$path}\": {\n";

            foreach ($dates as $date => $dateOffset) {
                $count = $merged[$offset + $dateOffset] and $buf[] = "        \"{$date}\": {$count},\n";
            }
            $buf and fwrite($out, substr($pathBuf . implode('', $buf),0,-2) . "\n    }");

            $offset += $dateCount;
        }
        fwrite($out, "\n}");
    }

    private static function parseChunk(
        string $inputPath,
        int $start,
        int $end,
        array $paths,
        array $dates,
        int $pathCount,
        int $dateCount,
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, min($remaining, static::BUFFER_SIZE));
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + 55);
                $counts[$paths[substr($chunk, $pos + 25, $nl - $pos - 51)] * $dateCount + $dates[substr($chunk, $nl - 25, 10)]]++;
                $pos = $nl + 1;
            }
        }

        return $counts;
    }
}
