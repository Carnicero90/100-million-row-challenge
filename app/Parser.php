<?php

namespace App;

use App\Commands\Visit;

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
    private const int WORKERS = 10;
    private const int BUFFER_SIZE = 163_840;

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

        $paths = array_map(fn($i) => substr($i->uri, 25), Visit::all());
        $pathCount = count($paths);

        $chunk = fread($fh, 8_388_608);

        $lastLineBreak = strrpos($chunk, "\n");
        $pathsByEntry = [];
        $pathCount = 0;
        $pos = 25;
        $tPos = strpos($chunk, 'T', $pos);
        $minDate = substr($chunk, $tPos - 7, 7);

        while ($pos < $lastLineBreak) {
            $tPos = strpos($chunk, 'T', $pos);
            $pathsByEntry[match ($tPos - $pos - 11) {
                4 => 183, 36 => 55, 42 => 59, 44 => 129, 47 => 219,
                9 => match ($chunk[$pos]) { 'p' => 121, 's' => 202 },
                40 => match ($chunk[$pos + 7]) { 'd' => 67, 'i' => 238 },
                45 => match ($chunk[$pos]) { 'c' => 82, 'a' => 130 },
                48 => match ($chunk[$pos]) { 'b' => 85, 'w' => 109 },
                7 => match ($chunk[$pos]) { 'p' => 51, 'h' => 123, 'g' => 167 },
                10 => match ($chunk[$pos]) { 'h' => 158, 's' => 168, 'y' => 226 },
                32 => match ($chunk[$pos + 2]) { 'e' => 107, 'a' => 125, '-' => 139 },
                8 => match ($chunk[$pos]) { 's' => 196, 'a' => 199, 'r' => 212, 'p' => 261 },
                35 => match ($chunk[$pos]) { 'r' => 14, 'b' => 95, 'a' => 170, 'p' => 266 },
                37 => match ($chunk[$pos]) {
                    'l' => 37, 'a' => match ($chunk[$pos + 2]) { 'l' => 69, 's' => 218 },
                    'c' => match ($chunk[$pos + 36]) { '1' => 135, '3' => 204 }, 'u' => 176,
                },
                38 => match ($chunk[$pos + 1]) { 'y' => 21, 'e' => 90, 'a' => 143, '-' => 235 },
                39 => match ($chunk[$pos]) { 'p' => 11, 'w' => 63, 'd' => 180, 't' => 217 },
                12 => match ($chunk[$pos + 2]) { 'w' => 114, 'r' => 128, 'd' => 200, 'i' => 223, 't' => 263 },
                34 => match ($chunk[$pos + 3]) { 's' => 28, 'e' => 29, 't' => 94, '-' => 197, 'c' => 207 },
                18 => match ($chunk[$pos + 4]) {
                    'n' => 38, '7' => match ($chunk[$pos + 5]) { '3' => 54, '4' => 75 },
                    't' => 61, 'y' => 83, '8' => match ($chunk[$pos + 5]) { '1' => 154, '2' => 187 }, 'r' => 209,
                },
                19 => match ($chunk[$pos + 1]) { 'y' => 23, 'a' => 36, '-' => 62, 'n' => 78, 't' => 106, 'p' => 192 },
                24 => match ($chunk[$pos + 4]) { 'm' => 19, 'y' => 49, 't' => 68, 'v' => 72, 'i' => 191, 'a' => 249 },
                28 => match ($chunk[$pos + 7]) { 'a' => 15, 'i' => 25, '-' => 33, 'm' => 88, 'n' => 99, 's' => 144, 'w' => 243 },
                22 => match ($chunk[$pos + 4]) {
                    'h' => 0, 'e' => match ($chunk[$pos]) { 'm' => 16, 'd' => 186 }, 'o' => 30, 'p' => 76,
                    '8' => match ($chunk[$pos + 6]) { 'i' => 84, 'b' => 100 },
                    's' => 256, '-' => match ($chunk[$pos]) { 'g' => 258, 'o' => 262 },
                },
                25 => match ($chunk[$pos]) {
                    'v' => 32, 'l' => 53, 'c' => 71,
                    't' => match ($chunk[$pos + 1]) { 'y' => 89, 'h' => 206 },
                    'w' => match ($chunk[$pos + 5]) { 'i' => 91, 'a' => 110 },
                    'd' => match ($chunk[$pos + 16]) { 'e' => 161, 'r' => 169 }, 'e' => 175,
                },
                31 => match ($chunk[$pos]) {
                    'u' => 56, 'l' => match ($chunk[$pos + 1]) { 'a' => 73, 'i' => 179 }, 'm' => 81,
                    'w' => match ($chunk[$pos + 2]) { 'y' => 98, 'e' => 116 },
                    'p' => match ($chunk[$pos + 4]) { '8' => 153, 'p' => 178 }, 'h' => 155, 'c' => 172,
                },
                33 => match ($chunk[$pos + 3]) {
                    'k' => match ($chunk[$pos + 32]) { '1' => 1, '2' => 2 },
                    '-' => match ($chunk[$pos]) { 'p' => 8, 'j' => 96 }, 's' => 41, 'r' => 48, 'e' => 92, 'i' => 138, 'u' => 182,
                },
                11 => match ($chunk[$pos + 10]) {
                    '9' => 60, 's' => match ($chunk[$pos]) { 'g' => 70, 'd' => 103, 'a' => 104, 'f' => 260 },
                    '0' => 77, '1' => 127, '2' => 157, '3' => 194, 'w' => 214, '4' => 221,
                },
                13 => match ($chunk[$pos + 12]) {
                    's' => match ($chunk[$pos]) { 'p' => 12, 'i' => 252 },
                    '3' => match ($chunk[$pos + 11]) { '7' => 44, '8' => 213 },
                    '4' => match ($chunk[$pos + 11]) { '7' => 74, '8' => 240 },
                    'k' => 145, '1' => 152, 'd' => match ($chunk[$pos]) { 'p' => 188, 'v' => 254 }, '2' => 190, '5' => 259,
                },
                14 => match ($chunk[$pos + 9]) { 's' => 105, 'e' => 111, 'g' => 118, 'w' => 141, 'f' => 160, 'h' => 177, 'l' => 208, '-' => 245 },
                15 => match ($chunk[$pos + 7]) {
                    'r' => match ($chunk[$pos + 14]) { '1' => 9, '2' => 17 }, 'p' => 27,
                    '-' => match ($chunk[$pos]) { 't' => 34, 'i' => 211 }, 'n' => match ($chunk[$pos]) { 'c' => 47, 't' => 64 },
                    'a' => match ($chunk[$pos]) { 't' => 86, 'p' => 233 }, 'm' => 108, 'i' => 112, 'd' => 140, 'e' => 195,
                },
                26 => match ($chunk[$pos + 3]) {
                    'e' => match ($chunk[$pos]) { 'o' => 13, 't' => 65 }, 'f' => 52,
                    'u' => match ($chunk[$pos]) { 't' => 124, 'r' => 244 },
                    't' => match ($chunk[$pos + 11]) { 'c' => 131, 'r' => 134 },
                    '-' => match ($chunk[$pos + 7]) { 'r' => 146, 'n' => 148 },
                    'd' => 185, 'g' => 237, 'c' => 264, 'm' => 267,
                },
                29 => match ($chunk[$pos + 2]) { 'e' => 22, 's' => 24, 'p' => 26, 'r' => 42, 'a' => 43, 'v' => 46, 'n' => 122, '-' => 198, 'd' => 232 },
                17 => match ($chunk[$pos + 16]) { ' ' => 39, 'c' => 115, 'g' => 142, 'o' => 162, '1' => 163, '2' => 164, '3' => 165, '4' => 166, 's' => 222, 't' => 230 },
                20 => match ($chunk[$pos + 2]) {
                    'p' => match ($chunk[$pos + 3]) { 's' => 18, '-' => 171 },
                    'o' => 35, 'n' => 50, 'e' => 66, 'g' => 201, 'm' => 205, 'r' => 229,
                    'i' => match ($chunk[$pos]) { 'b' => 236, 't' => 251 }, 'a' => 253, 'd' => 255,
                },
                21 => match ($chunk[$pos + 7]) {
                    't' => 31, 'g' => 57, 'l' => 58,
                    'a' => match ($chunk[$pos + 6]) { 'm' => 97, 'n' => 101 },
                    '-' => match ($chunk[$pos]) { 'o' => 132, 'e' => 234 },
                    's' => 150, 'm' => 181, 'd' => 231, 'i' => 242, 'c' => 265,
                },
                23 => match ($chunk[$pos + 5]) {
                    'c' => 4, 'e' => match ($chunk[$pos]) { 's' => 6, 'a' => 136 }, 's' => 79, '4' => 87, '-' => 113, 't' => 117,
                    'n' => match ($chunk[$pos]) { 'p' => 120, 'r' => 250 },
                    '1' => match ($chunk[$pos + 7]) { 'b' => 137, 'i' => 151 }, '2' => 184, 'o' => 248,
                },
                27 => match ($chunk[$pos + 26]) {
                    'o' => 40, 'r' => 119, 't' => 126, '1' => 133, '2' => 174, '3' => 210,
                    '4' => match ($chunk[$pos]) { 'p' => 225, 'n' => 227 }, 'd' => 246, '5' => 247, '6' => 257,
                },
                16 => match ($chunk[$pos + 10]) {
                    'm' => 3, 'l' => match ($chunk[$pos + 15]) { '4' => 5, '5' => 7 },
                    'u' => match ($chunk[$pos]) { 'a' => 20, 's' => 173 },
                    'e' => 80, 'i' => 149, 'k' => 203, 'o' => 215, 't' => 220, 'n' => 224, 'p' => 228, 'a' => 239,
                },
                30 => match ($chunk[$pos + 29]) { 's' => 10, 'm' => 45, '8' => 93, 'g' => 102, 'k' => 147, '2' => 156, 'e' => 159, 'n' => 189, '3' => 193, '4' => 216, '5' => 241 },
            }] ??= $pathCount++;
            $minDate = min(substr($chunk, $tPos - 7, 7), $minDate);
            $pos = $tPos + 41;
        }

        $fiveYearsInSeconds = 60 * 60 * 24 * 365 * 5;

        $dateCount = ((strtotime($minDate)+$fiveYearsInSeconds) - strtotime($minDate)) / 86400 + 1;
        $matrixSize = $dateCount * $pathCount;

        $cy = (int)$minDate[0] + 2020;
        $cm = (int)substr($minDate, 2, 2);
        $cd = (int)substr($minDate, 5, 2);

        $dates = [];


        for ($i = 0; $i < $dateCount; $i++) {
            $dates[($cy % 10) . '-' . ($cm < 10 ? '0' : '') . $cm . '-' . ($cd < 10 ? '0' : '') . $cd] = $i;
            $dim = $cm === 2
                ? ($cy % 4 === 0 && ($cy % 100 !== 0 || $cy % 400 === 0) ? 29 : 28)
                : ($cm === 4 || $cm === 6 || $cm === 9 || $cm === 11 ? 30 : 31);
            if (++$cd > $dim) { $cd = 1; if (++$cm > 12) { $cm = 1; ++$cy; } }
        }
        $dateStrings = array_flip($dates);

        $dateIds = array_combine($dateStrings, str_split(pack('v*', ...range(0, $dateCount-1)), 2));

        $pid = getmypid();
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $shmFiles = [];

        $merged = array_fill(0, $matrixSize, 0);
        $results = array_fill(0, $pathCount, '');

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shmFile = "{$shmDir}/part_{$w}";
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                fwrite(fopen($shmFile, 'wb'), self::parseChunk(
                    $inputPath,
                    $bounds[$w],
                    $bounds[$w + 1],
                    $pathsByEntry,
                    $dateIds,
                    $matrixSize,
                    $dateCount,
                    $merged,
                    $results,
                    )
                    );
                exit(0);
            }
            $shmFiles[$childPid] = $shmFile;
        }

        $merged = self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $pathsByEntry, $dateIds, $matrixSize, $dateCount, $merged, $results, true);

        $remaining = count($shmFiles);

        while ($remaining--) {
            $pid = pcntl_waitpid(-1, $status);
            $data = unpack('v*', file_get_contents($shmFiles[$pid]));
            unlink($shmFiles[$pid]);
            for ($i = 0; $i < $matrixSize; $i++) {
                $merged[$i] += $data[$i + 1];
            }
        }

        $pathsByEntry = array_flip($pathsByEntry);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0);
        fwrite($out, '{');

        $buf = [];
        for ($j = 0; $j < $dateCount; $j++) {
            if ($c=$merged[$j]) {
                $buf[] = "        \"202{$dateStrings[$j]}\": {$c},\n";
            }
        }
        if ($buf) {
            fwrite($out, "\n    \"\/blog\/{$paths[$pathsByEntry[0]]}\": {\n".substr(implode('', $buf),0,-2) . "\n    }");
        }

        $offset = 1;

        for ($i = $dateCount; $i < $matrixSize; $i+=$dateCount) {
            $curpath = $paths[$pathsByEntry[$offset]];
            $buf = [];
            $offset++;

            for ($j = 0; $j < $dateCount; $j++) {
                if ($c=$merged[$i+$j]) {
                    $buf[] = "        \"202{$dateStrings[$j]}\": {$c},\n";
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
        array $pathsByEntry,
        array $dateIds,
        int $matrixSize,
        int $dateCount,
        $counts,
        $results,
        bool $master = false,
    ) {
        $pathCount = intdiv($matrixSize, $dateCount);

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

            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $results[$pathsByEntry[match ($tPos - $pos - 11) {
                    4 => 183, 36 => 55, 42 => 59, 44 => 129, 47 => 219,
                    9 => match ($chunk[$pos]) { 'p' => 121, 's' => 202 },
                    40 => match ($chunk[$pos + 7]) { 'd' => 67, 'i' => 238 },
                    45 => match ($chunk[$pos]) { 'c' => 82, 'a' => 130 },
                    48 => match ($chunk[$pos]) { 'b' => 85, 'w' => 109 },
                    7 => match ($chunk[$pos]) { 'p' => 51, 'h' => 123, 'g' => 167 },
                    10 => match ($chunk[$pos]) { 'h' => 158, 's' => 168, 'y' => 226 },
                    32 => match ($chunk[$pos + 2]) { 'e' => 107, 'a' => 125, '-' => 139 },
                    8 => match ($chunk[$pos]) { 's' => 196, 'a' => 199, 'r' => 212, 'p' => 261 },
                    35 => match ($chunk[$pos]) { 'r' => 14, 'b' => 95, 'a' => 170, 'p' => 266 },
                    37 => match ($chunk[$pos]) {
                        'l' => 37, 'a' => match ($chunk[$pos + 2]) { 'l' => 69, 's' => 218 },
                        'c' => match ($chunk[$pos + 36]) { '1' => 135, '3' => 204 }, 'u' => 176,
                    },
                    38 => match ($chunk[$pos + 1]) { 'y' => 21, 'e' => 90, 'a' => 143, '-' => 235 },
                    39 => match ($chunk[$pos]) { 'p' => 11, 'w' => 63, 'd' => 180, 't' => 217 },
                    12 => match ($chunk[$pos + 2]) { 'w' => 114, 'r' => 128, 'd' => 200, 'i' => 223, 't' => 263 },
                    34 => match ($chunk[$pos + 3]) { 's' => 28, 'e' => 29, 't' => 94, '-' => 197, 'c' => 207 },
                    18 => match ($chunk[$pos + 4]) {
                        'n' => 38, '7' => match ($chunk[$pos + 5]) { '3' => 54, '4' => 75 },
                        't' => 61, 'y' => 83, '8' => match ($chunk[$pos + 5]) { '1' => 154, '2' => 187 }, 'r' => 209,
                    },
                    19 => match ($chunk[$pos + 1]) { 'y' => 23, 'a' => 36, '-' => 62, 'n' => 78, 't' => 106, 'p' => 192 },
                    24 => match ($chunk[$pos + 4]) { 'm' => 19, 'y' => 49, 't' => 68, 'v' => 72, 'i' => 191, 'a' => 249 },
                    28 => match ($chunk[$pos + 7]) { 'a' => 15, 'i' => 25, '-' => 33, 'm' => 88, 'n' => 99, 's' => 144, 'w' => 243 },
                    22 => match ($chunk[$pos + 4]) {
                        'h' => 0, 'e' => match ($chunk[$pos]) { 'm' => 16, 'd' => 186 }, 'o' => 30, 'p' => 76,
                        '8' => match ($chunk[$pos + 6]) { 'i' => 84, 'b' => 100 },
                        's' => 256, '-' => match ($chunk[$pos]) { 'g' => 258, 'o' => 262 },
                    },
                    25 => match ($chunk[$pos]) {
                        'v' => 32, 'l' => 53, 'c' => 71,
                        't' => match ($chunk[$pos + 1]) { 'y' => 89, 'h' => 206 },
                        'w' => match ($chunk[$pos + 5]) { 'i' => 91, 'a' => 110 },
                        'd' => match ($chunk[$pos + 16]) { 'e' => 161, 'r' => 169 }, 'e' => 175,
                    },
                    31 => match ($chunk[$pos]) {
                        'u' => 56, 'l' => match ($chunk[$pos + 1]) { 'a' => 73, 'i' => 179 }, 'm' => 81,
                        'w' => match ($chunk[$pos + 2]) { 'y' => 98, 'e' => 116 },
                        'p' => match ($chunk[$pos + 4]) { '8' => 153, 'p' => 178 }, 'h' => 155, 'c' => 172,
                    },
                    33 => match ($chunk[$pos + 3]) {
                        'k' => match ($chunk[$pos + 32]) { '1' => 1, '2' => 2 },
                        '-' => match ($chunk[$pos]) { 'p' => 8, 'j' => 96 }, 's' => 41, 'r' => 48, 'e' => 92, 'i' => 138, 'u' => 182,
                    },
                    11 => match ($chunk[$pos + 10]) {
                        '9' => 60, 's' => match ($chunk[$pos]) { 'g' => 70, 'd' => 103, 'a' => 104, 'f' => 260 },
                        '0' => 77, '1' => 127, '2' => 157, '3' => 194, 'w' => 214, '4' => 221,
                    },
                    13 => match ($chunk[$pos + 12]) {
                        's' => match ($chunk[$pos]) { 'p' => 12, 'i' => 252 },
                        '3' => match ($chunk[$pos + 11]) { '7' => 44, '8' => 213 },
                        '4' => match ($chunk[$pos + 11]) { '7' => 74, '8' => 240 },
                        'k' => 145, '1' => 152, 'd' => match ($chunk[$pos]) { 'p' => 188, 'v' => 254 }, '2' => 190, '5' => 259,
                    },
                    14 => match ($chunk[$pos + 9]) { 's' => 105, 'e' => 111, 'g' => 118, 'w' => 141, 'f' => 160, 'h' => 177, 'l' => 208, '-' => 245 },
                    15 => match ($chunk[$pos + 7]) {
                        'r' => match ($chunk[$pos + 14]) { '1' => 9, '2' => 17 }, 'p' => 27,
                        '-' => match ($chunk[$pos]) { 't' => 34, 'i' => 211 }, 'n' => match ($chunk[$pos]) { 'c' => 47, 't' => 64 },
                        'a' => match ($chunk[$pos]) { 't' => 86, 'p' => 233 }, 'm' => 108, 'i' => 112, 'd' => 140, 'e' => 195,
                    },
                    26 => match ($chunk[$pos + 3]) {
                        'e' => match ($chunk[$pos]) { 'o' => 13, 't' => 65 }, 'f' => 52,
                        'u' => match ($chunk[$pos]) { 't' => 124, 'r' => 244 },
                        't' => match ($chunk[$pos + 11]) { 'c' => 131, 'r' => 134 },
                        '-' => match ($chunk[$pos + 7]) { 'r' => 146, 'n' => 148 },
                        'd' => 185, 'g' => 237, 'c' => 264, 'm' => 267,
                    },
                    29 => match ($chunk[$pos + 2]) { 'e' => 22, 's' => 24, 'p' => 26, 'r' => 42, 'a' => 43, 'v' => 46, 'n' => 122, '-' => 198, 'd' => 232 },
                    17 => match ($chunk[$pos + 16]) { ' ' => 39, 'c' => 115, 'g' => 142, 'o' => 162, '1' => 163, '2' => 164, '3' => 165, '4' => 166, 's' => 222, 't' => 230 },
                    20 => match ($chunk[$pos + 2]) {
                        'p' => match ($chunk[$pos + 3]) { 's' => 18, '-' => 171 },
                        'o' => 35, 'n' => 50, 'e' => 66, 'g' => 201, 'm' => 205, 'r' => 229,
                        'i' => match ($chunk[$pos]) { 'b' => 236, 't' => 251 }, 'a' => 253, 'd' => 255,
                    },
                    21 => match ($chunk[$pos + 7]) {
                        't' => 31, 'g' => 57, 'l' => 58,
                        'a' => match ($chunk[$pos + 6]) { 'm' => 97, 'n' => 101 },
                        '-' => match ($chunk[$pos]) { 'o' => 132, 'e' => 234 },
                        's' => 150, 'm' => 181, 'd' => 231, 'i' => 242, 'c' => 265,
                    },
                    23 => match ($chunk[$pos + 5]) {
                        'c' => 4, 'e' => match ($chunk[$pos]) { 's' => 6, 'a' => 136 }, 's' => 79, '4' => 87, '-' => 113, 't' => 117,
                        'n' => match ($chunk[$pos]) { 'p' => 120, 'r' => 250 },
                        '1' => match ($chunk[$pos + 7]) { 'b' => 137, 'i' => 151 }, '2' => 184, 'o' => 248,
                    },
                    27 => match ($chunk[$pos + 26]) {
                        'o' => 40, 'r' => 119, 't' => 126, '1' => 133, '2' => 174, '3' => 210,
                        '4' => match ($chunk[$pos]) { 'p' => 225, 'n' => 227 }, 'd' => 246, '5' => 247, '6' => 257,
                    },
                    16 => match ($chunk[$pos + 10]) {
                        'm' => 3, 'l' => match ($chunk[$pos + 15]) { '4' => 5, '5' => 7 },
                        'u' => match ($chunk[$pos]) { 'a' => 20, 's' => 173 },
                        'e' => 80, 'i' => 149, 'k' => 203, 'o' => 215, 't' => 220, 'n' => 224, 'p' => 228, 'a' => 239,
                    },
                    30 => match ($chunk[$pos + 29]) { 's' => 10, 'm' => 45, '8' => 93, 'g' => 102, 'k' => 147, '2' => 156, 'e' => 159, 'n' => 189, '3' => 193, '4' => 216, '5' => 241 },
                }]] .= $dateIds[substr($chunk, $tPos - 7, 7)];
                $pos = $tPos + 41;
            }
        }
        if ($remaining) {
            $chunk = fread($handle, $remaining);
            $lastLineBreak = strlen($chunk);
            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $results[$pathsByEntry[match ($tPos - $pos - 11) {
                    4 => 183, 36 => 55, 42 => 59, 44 => 129, 47 => 219,
                    9 => match ($chunk[$pos]) { 'p' => 121, 's' => 202 },
                    40 => match ($chunk[$pos + 7]) { 'd' => 67, 'i' => 238 },
                    45 => match ($chunk[$pos]) { 'c' => 82, 'a' => 130 },
                    48 => match ($chunk[$pos]) { 'b' => 85, 'w' => 109 },
                    7 => match ($chunk[$pos]) { 'p' => 51, 'h' => 123, 'g' => 167 },
                    10 => match ($chunk[$pos]) { 'h' => 158, 's' => 168, 'y' => 226 },
                    32 => match ($chunk[$pos + 2]) { 'e' => 107, 'a' => 125, '-' => 139 },
                    8 => match ($chunk[$pos]) { 's' => 196, 'a' => 199, 'r' => 212, 'p' => 261 },
                    35 => match ($chunk[$pos]) { 'r' => 14, 'b' => 95, 'a' => 170, 'p' => 266 },
                    37 => match ($chunk[$pos]) {
                        'l' => 37, 'a' => match ($chunk[$pos + 2]) { 'l' => 69, 's' => 218 },
                        'c' => match ($chunk[$pos + 36]) { '1' => 135, '3' => 204 }, 'u' => 176,
                    },
                    38 => match ($chunk[$pos + 1]) { 'y' => 21, 'e' => 90, 'a' => 143, '-' => 235 },
                    39 => match ($chunk[$pos]) { 'p' => 11, 'w' => 63, 'd' => 180, 't' => 217 },
                    12 => match ($chunk[$pos + 2]) { 'w' => 114, 'r' => 128, 'd' => 200, 'i' => 223, 't' => 263 },
                    34 => match ($chunk[$pos + 3]) { 's' => 28, 'e' => 29, 't' => 94, '-' => 197, 'c' => 207 },
                    18 => match ($chunk[$pos + 4]) {
                        'n' => 38, '7' => match ($chunk[$pos + 5]) { '3' => 54, '4' => 75 },
                        't' => 61, 'y' => 83, '8' => match ($chunk[$pos + 5]) { '1' => 154, '2' => 187 }, 'r' => 209,
                    },
                    19 => match ($chunk[$pos + 1]) { 'y' => 23, 'a' => 36, '-' => 62, 'n' => 78, 't' => 106, 'p' => 192 },
                    24 => match ($chunk[$pos + 4]) { 'm' => 19, 'y' => 49, 't' => 68, 'v' => 72, 'i' => 191, 'a' => 249 },
                    28 => match ($chunk[$pos + 7]) { 'a' => 15, 'i' => 25, '-' => 33, 'm' => 88, 'n' => 99, 's' => 144, 'w' => 243 },
                    22 => match ($chunk[$pos + 4]) {
                        'h' => 0, 'e' => match ($chunk[$pos]) { 'm' => 16, 'd' => 186 }, 'o' => 30, 'p' => 76,
                        '8' => match ($chunk[$pos + 6]) { 'i' => 84, 'b' => 100 },
                        's' => 256, '-' => match ($chunk[$pos]) { 'g' => 258, 'o' => 262 },
                    },
                    25 => match ($chunk[$pos]) {
                        'v' => 32, 'l' => 53, 'c' => 71,
                        't' => match ($chunk[$pos + 1]) { 'y' => 89, 'h' => 206 },
                        'w' => match ($chunk[$pos + 5]) { 'i' => 91, 'a' => 110 },
                        'd' => match ($chunk[$pos + 16]) { 'e' => 161, 'r' => 169 }, 'e' => 175,
                    },
                    31 => match ($chunk[$pos]) {
                        'u' => 56, 'l' => match ($chunk[$pos + 1]) { 'a' => 73, 'i' => 179 }, 'm' => 81,
                        'w' => match ($chunk[$pos + 2]) { 'y' => 98, 'e' => 116 },
                        'p' => match ($chunk[$pos + 4]) { '8' => 153, 'p' => 178 }, 'h' => 155, 'c' => 172,
                    },
                    33 => match ($chunk[$pos + 3]) {
                        'k' => match ($chunk[$pos + 32]) { '1' => 1, '2' => 2 },
                        '-' => match ($chunk[$pos]) { 'p' => 8, 'j' => 96 }, 's' => 41, 'r' => 48, 'e' => 92, 'i' => 138, 'u' => 182,
                    },
                    11 => match ($chunk[$pos + 10]) {
                        '9' => 60, 's' => match ($chunk[$pos]) { 'g' => 70, 'd' => 103, 'a' => 104, 'f' => 260 },
                        '0' => 77, '1' => 127, '2' => 157, '3' => 194, 'w' => 214, '4' => 221,
                    },
                    13 => match ($chunk[$pos + 12]) {
                        's' => match ($chunk[$pos]) { 'p' => 12, 'i' => 252 },
                        '3' => match ($chunk[$pos + 11]) { '7' => 44, '8' => 213 },
                        '4' => match ($chunk[$pos + 11]) { '7' => 74, '8' => 240 },
                        'k' => 145, '1' => 152, 'd' => match ($chunk[$pos]) { 'p' => 188, 'v' => 254 }, '2' => 190, '5' => 259,
                    },
                    14 => match ($chunk[$pos + 9]) { 's' => 105, 'e' => 111, 'g' => 118, 'w' => 141, 'f' => 160, 'h' => 177, 'l' => 208, '-' => 245 },
                    15 => match ($chunk[$pos + 7]) {
                        'r' => match ($chunk[$pos + 14]) { '1' => 9, '2' => 17 }, 'p' => 27,
                        '-' => match ($chunk[$pos]) { 't' => 34, 'i' => 211 }, 'n' => match ($chunk[$pos]) { 'c' => 47, 't' => 64 },
                        'a' => match ($chunk[$pos]) { 't' => 86, 'p' => 233 }, 'm' => 108, 'i' => 112, 'd' => 140, 'e' => 195,
                    },
                    26 => match ($chunk[$pos + 3]) {
                        'e' => match ($chunk[$pos]) { 'o' => 13, 't' => 65 }, 'f' => 52,
                        'u' => match ($chunk[$pos]) { 't' => 124, 'r' => 244 },
                        't' => match ($chunk[$pos + 11]) { 'c' => 131, 'r' => 134 },
                        '-' => match ($chunk[$pos + 7]) { 'r' => 146, 'n' => 148 },
                        'd' => 185, 'g' => 237, 'c' => 264, 'm' => 267,
                    },
                    29 => match ($chunk[$pos + 2]) { 'e' => 22, 's' => 24, 'p' => 26, 'r' => 42, 'a' => 43, 'v' => 46, 'n' => 122, '-' => 198, 'd' => 232 },
                    17 => match ($chunk[$pos + 16]) { ' ' => 39, 'c' => 115, 'g' => 142, 'o' => 162, '1' => 163, '2' => 164, '3' => 165, '4' => 166, 's' => 222, 't' => 230 },
                    20 => match ($chunk[$pos + 2]) {
                        'p' => match ($chunk[$pos + 3]) { 's' => 18, '-' => 171 },
                        'o' => 35, 'n' => 50, 'e' => 66, 'g' => 201, 'm' => 205, 'r' => 229,
                        'i' => match ($chunk[$pos]) { 'b' => 236, 't' => 251 }, 'a' => 253, 'd' => 255,
                    },
                    21 => match ($chunk[$pos + 7]) {
                        't' => 31, 'g' => 57, 'l' => 58,
                        'a' => match ($chunk[$pos + 6]) { 'm' => 97, 'n' => 101 },
                        '-' => match ($chunk[$pos]) { 'o' => 132, 'e' => 234 },
                        's' => 150, 'm' => 181, 'd' => 231, 'i' => 242, 'c' => 265,
                    },
                    23 => match ($chunk[$pos + 5]) {
                        'c' => 4, 'e' => match ($chunk[$pos]) { 's' => 6, 'a' => 136 }, 's' => 79, '4' => 87, '-' => 113, 't' => 117,
                        'n' => match ($chunk[$pos]) { 'p' => 120, 'r' => 250 },
                        '1' => match ($chunk[$pos + 7]) { 'b' => 137, 'i' => 151 }, '2' => 184, 'o' => 248,
                    },
                    27 => match ($chunk[$pos + 26]) {
                        'o' => 40, 'r' => 119, 't' => 126, '1' => 133, '2' => 174, '3' => 210,
                        '4' => match ($chunk[$pos]) { 'p' => 225, 'n' => 227 }, 'd' => 246, '5' => 247, '6' => 257,
                    },
                    16 => match ($chunk[$pos + 10]) {
                        'm' => 3, 'l' => match ($chunk[$pos + 15]) { '4' => 5, '5' => 7 },
                        'u' => match ($chunk[$pos]) { 'a' => 20, 's' => 173 },
                        'e' => 80, 'i' => 149, 'k' => 203, 'o' => 215, 't' => 220, 'n' => 224, 'p' => 228, 'a' => 239,
                    },
                    30 => match ($chunk[$pos + 29]) { 's' => 10, 'm' => 45, '8' => 93, 'g' => 102, 'k' => 147, '2' => 156, 'e' => 159, 'n' => 189, '3' => 193, '4' => 216, '5' => 241 },
                }]] .= $dateIds[substr($chunk, $tPos - 7, 7)];
                $pos = $tPos + 41;
            }
        }

        for ($i = 0; $i < $pathCount; $i++) {
            $pathOffset = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $results[$i])) as $dateOffset => $c) {
                $counts[$pathOffset + $dateOffset] = $c;
            }
        }
        if ($master) {
            return $counts;
        }

        return pack('v*', ...$counts);
    }
}
