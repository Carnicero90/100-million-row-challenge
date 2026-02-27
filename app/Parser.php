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
        $pos = 25;
        $tPos = strpos($chunk, 'T', $pos);
        $minDate = substr($chunk, $tPos - 10, 10);

        while ($pos < $lastLineBreak) {
            $tPos = strpos($chunk, 'T', $pos);
            $path = substr($chunk, $pos, $tPos - $pos -11 );
            $date = substr($chunk, $tPos - 10, 10);

            if (!isset($paths[$path])) $paths[$path] = $pathCount++;
            $minDate = min($date, $minDate);

            $pos = $tPos + 41;
        }

        $fiveYearsInSeconds = 60 * 60 * 24 * 365 * 5;

        $dateCount = ((strtotime($minDate)+$fiveYearsInSeconds) - strtotime($minDate)) / 86400 + 1;
        $matrixSize = $dateCount * $pathCount;
        $minDate = strtotime($minDate);

        $dates = [];
        for ($i = 0; $i < $dateCount; $i++) {
            $dates[gmdate('Y-m-d', $minDate + $i * 86400)] = $i;
        }

        $pid = getmypid();
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $shmFiles = [];

        $pids = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shmFile = "{$shmDir}/parse_{$pid}_{$w}";
            $shmFiles[] = $shmFile;
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                fwrite(fopen($shmFile, 'wb'), pack('v*', ...self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $matrixSize, $dateCount)));
                exit(0);
            }
            $pids[] = $childPid;
        }

        $merged = self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dates, $matrixSize, $dateCount);

        $pidToIndex = array_flip($pids);
        $remaining = count($pids);
        while ($remaining--) {
            $pid = pcntl_waitpid(-1, $status);
            $k = $pidToIndex[$pid];
            $wCounts = unpack('v*', file_get_contents($shmFiles[$k]));
            unlink($shmFiles[$k]);
            $j = 0;
            foreach ($wCounts as $v) {
                $merged[$j++] += $v;
            }
        }

        $dateStrings = array_flip($dates);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0);
        fwrite($out, '{');

        $paths = array_keys($paths);

        $buf = [];
        for ($j = 0; $j < $dateCount; $j++) {
            if ($c=$merged[$j]) {
                $buf[] = "        \"{$dateStrings[$j]}\": {$c},\n";
            }
        }
        if ($buf) {
            fwrite($out, "\n    \"\/blog\/{$paths[0]}\": {\n".substr(implode('', $buf),0,-2) . "\n    }");
        }

        $offset = 1;

        for ($i = $dateCount; $i < $matrixSize; $i+=$dateCount) {
            $curpath = $paths[$offset];
            $buf = [];
            $offset++;

            for ($j = 0; $j < $dateCount; $j++) {
                if ($c=$merged[$i+$j]) {
                    $buf[] = "        \"{$dateStrings[$j]}\": {$c},\n";
                }
            }

            if ($buf) {
                fwrite($out, ",\n    \"\/blog\/{$curpath}\": {\n".substr(implode('', $buf),0,-2) . "\n    }");
            }
        }

        fwrite($out, "\n}");
    }

    private static function parseChunk(
        string $inputPath,
        int $start,
        int $end,
        array $paths,
        array $dates,
        int $matrixSize,
        int $dateCount,
    ): array {
        $counts = array_fill(0, $matrixSize, 0);

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
