# Wormhole MemTableの試作品

2026-11-19。Native C++で書いた実験用の追加バックエンド。
既存の既定値は変更せず、明示的なfactoryまたはベンチの`--backend wormhole`で選ぶ。

## 実装した範囲

- 128キー単位の葉リストと、短縮アンカーの全prefixを保持するハッシュ索引。
- prefix長の二分探索と、子のbitmap・部分木の最右葉を使った対象葉の検索。不在キーの範囲開始位置も検索できる。
- 分割の左最大・右最小キーのLCP長+1まで右最小を短縮してアンカーにする。終端を明示してアンカー同士のprefix関係を許容するため、0x00を含む任意のバイナリキーに予約トークンやfat leafは不要。
- 葉の点操作は16-bit tag順の配列を検索する。キー順のslot配列は16件ずつrun化し、書込み中の同サイズrunのmergeを64件以下に制限する。scan/splitでは全runを合流して順序を確定する。通常のremoveは既存coordinatorと同じくtombstone版の追加とする。
- シャード内の構造更新とmutable scanの捕捉をbackend全体のexclusive lockで直列化し、点読取りはshared lockで保護する。
- mutable scanは指定範囲の可視なRecordViewを最初のresumeで集め、lockを解放してからyieldする。最初の32行はcoroutine frame内に保持し、それを超える範囲だけvectorへ退避する。更新で版リングが上書きされても、捕捉済みレコード本体は世代のarenaで生き続ける。
- freeze時に未整列の葉を確定し、その後の走査は不変の葉をストリーム処理する。SSTには全保持版を渡す。
- application順・sequence順、設定可能な有界保持（既定4）と全履歴保持、同一seqの再適用、Blob参照等のrecord flags。
- splitで必要なmetadataとdirectoryの容量を全て準備し、追加確保が不要になってから構造変更をlock下で公開する。確保に失敗した場合も旧索引の論理状態を維持する。

ハッシュ索引はincremental CRC32Cに長さと64-bitのavalanche処理を加え、8タグ単位の独自directoryを使う。
CPUのSSE4.2対応を検出して8/4/2/1-byteのCRC命令を選択し、未対応環境は同じ結果のportable実装を使う。
選択はbackend構築時に行い、lookup中の各prefixでCPU判定を繰り返さない。
これは世代内の索引用hashであり、保存レコードのfingerprint、SSTのchecksum、保存形式は変更しない。
SSE2対応環境では8個の16-bitタグを同時比較し、他環境ではscalar実装を使う。
ポインタはフル幅で別配列に置く。全hash衝突はgroup linear probingで処理し、完全prefix比較で確認する。
衝突を解消するまで無制限に拡張する方式ではない。
暗号学的ハッシュではなく、敵対的な入力での性能上限を保証する設計でもない。

## 論文から取り込んだ最適化

2026-11-19の初版に続き、[原論文](https://ranger.uta.edu/~sjiang/pubs/papers/wu19-wormhole.pdf)の§3.1/§3.2とアンカー短縮の考え方を追加した。

| 項目 | Nativeでの実装 |
|---|---|
| MetaTrieHTのTagMatching | 8タグのgroupを先に調べ、LPM探索中はタグ一致を仮採用。最終prefixを完全比較し、不一致なら探索全体を完全比較でやり直す |
| IncHashing | 最後に一致したprefixのhash状態から必要な区間だけ延長。リクエストごとの全prefix hash配列と長いキーでのvector確保を除去 |
| 葉のSortByTag / DirectPos | 16-bit tag順の配列で`count * tag / 65536`から前後を調整し、同一tag候補を全て完全キー比較する |
| 葉の遅延sort | 新規キーをslotへ追記し、未整列16件をrun化。同サイズの小さいrunをmergeし、scan/split/freeze時に残りを合流 |
| 短縮アンカー | 左最大より大きい右最小の最短prefixを使用。任意バイナリの終端表現は初版の方式を継続 |

split時のtag索引は既存のtag順を線形に左右へ分け、新規キーのtagだけを挿入する。
キーごとの再挿入で両側の索引を作り直さず、分割に集中する処理を抑える。
論文のようにscan/splitだけを待つ方式では新規putのP99が悪化したため、Native版では16件ずつrun化し、その場のmergeも64件以下に制限する。

原論文の48-bit packed pointerは使用していない。
§2.5の葉単位ロックとQSBR/RCUも今回の変更には含めず、snapshot保護と失敗時の公開手順を維持する。
論文にある全ての実装・並行性の最適化を再現した版ではない。

## scan用メモリとCRCの追加改善

2026-11-19。iterator用arenaの既定初期サイズを64 KiBから4 KiBへ変更した。
ctorの引数は維持し、明示したarenaサイズはそのまま使う。
32-byte以下の開始・終了キーと最初の32行をiterator自身の内部に保持し、短いscanの個別heap確保を減らす。
長いキーや33行以上の結果には動的領域を使い、全件のsnapshot捕捉と停止中の寿命を維持する。
thread-localな共有bufferは使わず、generatorを別threadで直列resumeできる契約を維持する。

## 葉内mergeとEntry配置の追加改善

2026-11-19。整列済みprefixを16件の追加ごとに全mergeする方式から、小さいrunを段階的に合流する方式へ変更した。
書込み中は同サイズかつ合計64件以下の末尾runだけをmergeする。
scan/split/freezeでは隣接runの合計件数が小さい組から合流し、単一のキー順へ確定する。
runとslot配列は葉内の固定配列で、sort/mergeは追加確保なしで行う。

per-key Entryには、元の不変キーrecordへの参照、履歴head、ring参照、head/count、tagをまとめて配置する。
旧Entryから別VersionChainを引くpointerを除去し、有界ringはEntry直後に一度だけ確保する。
旧Entry/VersionChainの2回のarena確保を1回にし、二重にサイズ計上しない。
OwnedRecord本体は移動せず、ringから外れたrecordも世代のarenaに保持してアンカーと停止中snapshotの寿命を維持する。

`OwnedRecord`の既存制約に合わせ、key/valueの合計は65535バイト以下。
空キー、0x00/0xff、65520バイトの共通prefixを持つ129キー、空の値を伴う65535バイトキーをテスト対象にする。

## 使い方

```cpp
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/WormholeMemTable.hpp"

using namespace akkaradb::engine::memtable;
MemTable::Options options;
options.backendFactory = []() -> std::unique_ptr<IMemTable> {
    return std::make_unique<WormholeMemTable>();
};
// SSTと接続する場合はseqによる版の優先順位を使う。
options.sequenceOrderedVersions = true;
```

`maxVersionsPerKey`をfactoryに渡す場合は、`backendFactoryWithOptions`から
`WormholeMemTable`の第5引数に`MemTableBackendOptions`を渡す。

```powershell
.\builds\read-scaling\bin\akkaradb_akkengine_latency_benchmark.exe --storage-only t --backend wormhole --sst f --keys 100000 --iterations 100000 --scan-rows 32 --cpu 0 --csv-dir results/NEW-DIRECTORY
python -B benchmarks/tools/run_storage_module_experiment.py builds/read-scaling/bin results/NEW-COMPARISON --backends art wormhole --keys 100000 --iterations 100000 --repeats 3 --flush-samples 3 --no-profiles --cpu 0
```

出力先は既存ディレクトリと重複しない名前を使う。

## 検証対象

`akkaradb_wormhole_memtable_smoke_test`はmapの参照モデルとの点・範囲・版の照合、
アンカーが他のアンカーのprefixになるケース、長いバイナリキー、snapshotの中断と別threadでの直列resume、
有界保持を超える更新、全保持版flush、freeze後の書込み拒否、メモリ計上、
分割とhash拡張中の割当て失敗・再試行、並行readerを扱う。
追加テストは同一full hashの80 prefix、directory拡張の各確保失敗、仮LPMの誤一致からroot/正しいprefixへの再探索、
同じ16-bit tagの異なる葉キー、末尾0x00のprefix連鎖を扱う。Windows clang-clのAddressSanitizerでも直接テストを確認した。
追加のCRCテストはbit単位の参照計算、既知vector、長さ0〜129と255/256/257/4095/4096/65535、16通りの非整列offset、複数seed、portable/hardware混在の分割入力を照合する。
短いscanの確保量、引数の変更後に初回resumeするboundsの所有、0/1/31/32/33行の境界も検証する。
runについては、不規則な挿入と途中の範囲scan、scan後に残る端数prefix、pending runを残したfreeze/flushを参照モデルと比較する。
近接配置したringは容量1/3/17/65535、out-of-order/同一seq更新、inline/arena payload、tombstoneを検証する。
割当て失敗のsweepは有界保持と全履歴保持の両方で行う。

既存の`akkaradb_sst_memtable_safety_smoke_test`にも4番目のbackendとして追加した。
coordinatorの版の優先順位、公開frontier、世代compaction、stream/vector両方のflush、SSTの再open/recoverを既存のケースで比較する。
CLI smokeも4方式を扱う。

## 現段階の制限

- 本番採用前の試作。直接テストのASanは確認したが、長期運転、TSan/UBSan、他OSでの検証は今後必要。
- ロックの細粒度化、QSBR/RCU、ポインタ圧縮は実装していない。CRC命令の高速化はx86 SSE4.2で、他環境はportable実装を使う。
- アンカーは短縮したが、非常に長い共通prefixではmetadataと比較コストが大きい。点操作はキー全体のタグ計算も含むため、探索全体を無条件にO(log L)時間とみなさない。
- mutable scanはexclusive lock下で結果をmaterializeするため、長い範囲では一時メモリと他操作の待ち時間が増える。
- 遅延sortはscan/split/freezeへ仕事を移す。平均スループットが改善しても、新規putのP99等が悪化し得るため、分位点と併せて判断する。
- 個別ノードを早期解放せず、未公開・retiredのarena確保も世代の破棄まで保持する。サイズ計上はallocatorの実使用量やプロセスRSSではない。
- 既存ARTを上回ることや、SST込み90万件/sの達成を前提にしない。同条件の測定で判断する。

[導入調査](wormhole-memtable-feasibility.md) /
[クラス](../../akkara/akkengine/include/akk/engine/memtable/WormholeMemTable.hpp) /
[実装](../../akkara/akkengine/src/engine/memtable/WormholeMemTable.cpp) /
[タグ索引](../../akkara/akkengine/include/akk/engine/memtable/detail/WormholeIndex.hpp) /
[CRCとCPU選択](../../akkara/akkengine/include/akk/engine/memtable/detail/WormholeHash.hpp) /
[直接回帰テスト](../../benchmarks/smoke/wormhole_memtable_smoke_test.cpp)
