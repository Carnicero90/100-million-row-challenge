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
use function preg_match_all;
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
    private const string REGEXP = '/https:\/\/stitcher\.io\/blog\/([^,]+),(\d{4}-\d{2}-\d{2})T/';

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

        $chunk = fread($fh, self::BUFFER_SIZE);

        $paths = [];
        $pathCount = 0;
        $dates = [];
        $dateCount = 0;

        preg_match_all(self::REGEXP, $chunk, $matches, PREG_SET_ORDER, 0);
        foreach ($matches as $m) {
            if (!isset($paths[$m[1]])) $paths[$m[1]] = $pathCount++;
            if (!isset($dates[$m[2]])) $dates[$m[2]] = $dateCount++;
        }

        ksort($dates);

        $pid = getmypid();
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $shmFiles = [];

        // Fork tutti i WORKERS come figli (incluso l'ultimo chunk)
        $pids = [];
        for ($w = 0; $w < self::WORKERS; $w++) {
            $shmFile = "{$shmDir}/parse_{$pid}_{$w}";
            $shmFiles[] = $shmFile;
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                fwrite(fopen($shmFile, 'wb'), pack('v*', ...self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $pathCount, $dateCount)));
                exit(0);
            }
            $pids[] = $childPid;
        }

        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $remaining = $pids;

        while ($remaining) {
            foreach ($remaining as $k => $childPid) {
                $res = pcntl_waitpid($childPid, $status, WNOHANG);
                if ($res > 0) {
                    $wCounts = unpack('v*', file_get_contents($shmFiles[$k]));
                    unlink($shmFiles[$k]);
                    $j = 0;
                    foreach ($wCounts as $v) {
                        $counts[$j++] += $v;
                    }
                    unset($remaining[$k]);
                }
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0);
        fwrite($out, '{');

        $offset = 0;
        foreach ($paths as $path => $_) {
            $buf = [];
            $pathBuf = ($offset ? ',' : '') . "\n    \"\/blog\/{$path}\": {\n";

            foreach ($dates as $date => $dateOffset) {
                $count = $counts[$offset + $dateOffset] and $buf[] = "        \"{$date}\": {$count},\n";
            }
            $buf and fwrite($out, substr($pathBuf . implode('', $buf), 0, -2) . "\n    }");

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

        while ($remaining > self::BUFFER_SIZE) {
            $chunk = fread($handle, self::BUFFER_SIZE);

            $remaining -= self::BUFFER_SIZE;

            $lastLineBreak = strrpos($chunk, "\n");

            if ($lastLineBreak < (self::BUFFER_SIZE - 1)) {
                $excess = self::BUFFER_SIZE - 1 - $lastLineBreak;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            // still trying my luck, since i guess $lastLineBreak could be false (very unlikely)
            preg_match_all(self::REGEXP, substr($chunk, 0, $lastLineBreak + 1), $matches, PREG_SET_ORDER, 0);
            foreach ($matches as $m) {
                $counts[$paths[$m[1]] * $dateCount + $dates[$m[2]]]++;
            }
        }
        if ($remaining) {
            preg_match_all(self::REGEXP, fread($handle, $remaining), $matches, PREG_SET_ORDER, 0);
            foreach ($matches as $m) {
                $counts[$paths[$m[1]] * $dateCount + $dates[$m[2]]]++;
            }
        }

        return $counts;
    }
}
