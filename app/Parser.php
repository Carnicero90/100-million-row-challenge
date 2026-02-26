<?php

namespace App;

use function array_fill;
use function fgets;
use function file_get_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function intdiv;
use function ksort;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const SEEK_SET;

final class Parser
{
    // private const STR_LENS_FOR_DUMMIES = [
    //     "https://stitcher.io/blog/`" => 25,
    //     "THH:MM:SS+00:00\n" => 16,
    //      "YYYY-MM-DD" => 10
    // ];
    private const int WORKERS = 8;
    private const int BUFFER_SIZE = 8_388_608;

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

        $lastLineBreak = strrpos($chunk, "\n");
        $paths = [];
        $pathCount = 0;
        $dates = [];
        $dateCount = 0;
        $pos = 25;

        while ($pos < $lastLineBreak) {
            $tPos = strpos($chunk, 'T', $pos);
            $path = substr($chunk, $pos, $tPos - $pos -11 );
            $date = substr($chunk, $tPos - 10, 10);

            if (!isset($paths[$path])) $paths[$path] = $pathCount++;
            if (!isset($dates[$date])) $dates[$date] = $dateCount++;

            $pos = $tPos + 41;
        }

        ksort($dates);

        $pid = getmypid();
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $shmFiles = [];

        $pids = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shmFile = "{$shmDir}/parse_{$pid}_{$w}";
            $shmFiles[] = $shmFile;
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                fwrite(fopen($shmFile, 'wb'), pack('V*', ...self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $pathCount, $dateCount)));
                exit(0);
            }
            $pids[] = $childPid;
        }

        $merged = self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $pathCount, $dateCount);

        foreach ($pids as $childPid) {
            pcntl_waitpid($childPid, $status);
        }

        foreach ($shmFiles as $shmFile) {
            $wCounts = unpack('V*', file_get_contents($shmFile));
            unlink($shmFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $merged[$j++] += $v;
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0);
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

        while ($remaining > static::BUFFER_SIZE) {
            $chunk = fread($handle, static::BUFFER_SIZE);

            $remaining -= static::BUFFER_SIZE;

            $lastLineBreak = strrpos($chunk, "\n");

            if ($lastLineBreak < (static::BUFFER_SIZE - 1)) {
                $excess = static::BUFFER_SIZE - 1 - $lastLineBreak;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $counts[$paths[substr($chunk, $pos, $tPos - $pos - 11)] * $dateCount + $dates[substr($chunk, $tPos - 10, 10)]]++;
                $pos = $tPos + 41;
            }
        }
        if ($remaining) {
            $chunk = fread($handle, $remaining);
            $lastLineBreak = strlen($chunk);
            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $counts[$paths[substr($chunk, $pos, $tPos - $pos - 11)] * $dateCount + $dates[substr($chunk, $tPos - 10, 10)]]++;
                $pos = $tPos + 41;
            }
        }

        return $counts;
    }
}
