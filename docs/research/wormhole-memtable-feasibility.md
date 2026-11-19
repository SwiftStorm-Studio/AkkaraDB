# WormholeをMemTableへ追加するための調査

調査日: 2026-11-18。対象はEuroSys 2019のインメモリ順序付き索引Wormhole。
2026-11-19追記: [Native C++の試作品](wormhole-memtable-prototype.md)を追加した。
以下は導入前の調査記録であり、試作品の実装範囲と制限はそちらを参照する。
参照実装は `wuxb45/wormhole` のコミット
`9d3d0d0491410867b9f80b9be39d1782bcc7950c`（調査時に確認したmaster）を基準にした。

## 結論

`WormholeMemTable : IMemTable` を新しい選択肢として追加する構成は可能と考える。
現時点の推奨は、論文の索引構造をNative C++に適合させ、まずシャード内の書き込みを直列化した安全な版を作ること。
参照C実装の直接組み込みには、Windows移植とAkkaraDB固有の版管理・iterator契約への適合が必要になる。
既存バックエンドの既定値や保存形式を変更する必要はない。

これは設計上の実現性の判断であり、Wormholeの性能・安全性をAkkaraDB上で検証した結果ではない。
今回の作業は資料と既存コードの調査までで、Wormholeの実装・ビルド・ベンチ測定は行っていない。

## 索引の構造

Wormholeは、キーの範囲を受け持つ葉ブロックのリストと、葉へ案内するprefix索引（MetaTrieHT）を組み合わせる。
prefix長に対する二分探索で最長一致を探し、葉内で目的のキーや範囲の開始位置を探す。
scanはそこから葉内と隣の葉へ進む。
論文の探索上の利点は、キー数Nに対する比較探索を減らすことにある。
`O(log L)`はprefix探索の考え方を示すもので、キーの読取り・ハッシュ計算や衝突処理まで無条件に定数費用になるわけではない。
[原論文](https://ranger.uta.edu/~sjiang/pubs/papers/wu19-wormhole.pdf)

```text
検索キー
  └─ prefixのハッシュ索引 → 対象の葉ブロック
                            ├─ 点検索: 葉内のキーを確認
                            └─ scan: 葉内を順に読む → 次の葉 → …
```

葉を単につないだB+Treeや、全キーをunordered_mapへ入れるだけの実装では、Wormholeの探索方式を実装したことにはならない。
存在しない開始キーも正しく案内できるprefix索引と、葉の境界を表すアンカーが中核になる。
原論文にはバイナリキー用のfat leafの議論があり、現在の参照実装とは境界処理の設計が異なる。
移植時は、論文の説明と現在の実装を混ぜず、採る方式を固定して不変条件を検証する。

## AkkaraDBに適合しやすい点

以下は現在のリポジトリのコードから確認した事項。

| 既存の仕組み | 導入への意味 |
|---|---|
| `IMemTable` と `backendFactory` / `backendFactoryWithOptions` | 既存3方式と並べて追加できる |
| `AkkEngineOptions::memtable` | 低レベルengine側にもMemTableの設定を渡せる |
| `MemTable::put` がシャードmutexで書き込みを直列化 | 初版から同一シャードへの複数writer対応を作る必要がない |
| `MemTable::remove` はtombstone付きput | 通常のremoveでキーや葉を物理削除する必要がない |
| 世代をfreezeしてSSTへ渡す | 書き込みを拒否した後に安定した全件走査ができる |
| `OwnedRecord` と `BufferArena` | レコードを不変に保ち、世代単位で寿命を管理する部品を再利用できる |
| `detail::VersionHistory` / `insertSequenceOrdered` | 公開frontier用の全履歴と、seq順の有界保持の部品を利用できる |

特に、通常のMemTable操作では物理的なキー削除がない。
初版ではsplitと挿入に集中し、世代を破棄するときにまとめて解放できるため、一般的なWormholeのdelete/merge実装を省ける。
ただし、これは新しく作るMemTable向け実装の単純化案であり、既存C実装の同期処理を無条件に取り除いてよいという意味ではない。

参照するローカルコード:

- [IMemTableの契約](../../akkara/akkengine/include/akk/engine/memtable/IMemTable.hpp)
- [MemTable設定・factory](../../akkara/akkengine/include/akk/engine/memtable/MemTable.hpp)
- [シャードの書き込み・remove・freeze/flush](../../akkara/akkengine/src/engine/memtable/MemTable.cpp)
- [版管理の共通部品](../../akkara/akkengine/include/akk/engine/memtable/detail/VersionHistory.hpp)
- [既存ARTのsnapshot保護](../../akkara/akkengine/src/engine/memtable/ARTMemTable.cpp)

## 参照C実装をそのまま採用する場合の課題

### Windowsへの適合

作者が挙げる対応OSはLinux・FreeBSD・macOSで、Windowsは含まれない。
[READMEの対応環境](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/README.md#supported-platforms)

`lib.h` / `ctypes.h` / `lib.c`にはPOSIXのthread・ページ確保、GNU拡張、C11 atomic、型幅やポインタ表現への依存がある。
例えば`unsigned long`を64ビット型として定義しているが、64ビットWindowsはLLP64でlongが32ビットのまま。
ビルド設定の書き換えだけで済む移植ではない。
48ビットのポインタ部分を使う表現も、そのままNative共通コードへ持ち込む前に見直す必要がある。
[参照lib.h](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/lib.h) /
[参照ctypes.h](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/ctypes.h) /
[MicrosoftのLLP64説明](https://learn.microsoft.com/en-us/windows/win32/winprog64/abstract-data-models)

### 停止しない失敗経路と、対応キーの制約

確認したREADMEは、特殊なmetadata hash衝突で挿入が拡張を繰り返し、メモリ待ちに入る既知制約を明記している。
長い共通prefixを共有するキー集合にも前提条件がある。
これはAkkaraDBの任意バイナリキーの契約へ、そのまま持ち込めない。
[作者の制約説明](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/README.md#limitations)

具体的には、既定の葉容量で129キーが65520バイト以上の共通prefixを持つ集合が対応範囲外になる。
AkkaraDBの`MemHdr16`では65535バイトまでのキーを表せるため、両者の前提は一致しない。
このような境界でも正しく終了するアンカーとoverflow処理を設計する必要がある。

ソースの`wormhmap_expand`にも、確保失敗時にsleepして再試行し続ける経路がある。
NativeのStatus/例外、書き込みの失敗通知、終了処理へ適合させるには、この動作を再設計する必要がある。
[参照wh.c](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/wh.c)

### iteratorと版管理

公開C APIにはキー・値のget/put/iteratorがあるが、AkkaraDBの`snapshotSeq`・全保持版flushをそのまま渡せるAPIはない。
それらはadapter側で実装する必要がある。
また、参照実装ではiteratorや参照のthreadごとの扱い、待機前のparkに注意が必要になる。
[公開API](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/wh.h) /
[参照管理の説明](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/README.md#avoid-blocking-writers-when-using-the-wormhole-api)

現行の`ArenaGenerator`で`co_yield`したままWormholeの葉ロックや活動中の参照を保持するadapterは避ける。
呼び出し側がiteratorを止めたり、同じスレッドで書き込んだりすると、停止待ちやロック順序の問題になり得る。

### 利用条件

確認したコミットではGPLv3またはMITの選択式とされ、MITのライセンスファイルも存在する。
再利用する場合は採る版を固定し、対象ファイルと必要な通知を確認する。
これは採用方法の調査であり、現在のプロジェクトへ外部実装や依存を追加したものではない。
[READMEのライセンス](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/README.md#license) /
[LICENSE-MIT](https://github.com/wuxb45/wormhole/blob/9d3d0d0491410867b9f80b9be39d1782bcc7950c/LICENSE-MIT)

## 推奨するNative版の初期設計

以下は既存コードへ適合させるための提案であり、実装済みの仕様ではない。

1. `WormholeMemTable`を追加し、既存バックエンドの既定値は維持する。
2. `user key -> VersionChain`を葉のエントリとする。削除は物理eraseせずtombstone版を追加する。
3. 初版の構造更新はbackendのexclusive lock、点読み取りはshared lockで保護する。外側のシャードlockだけでは通常の読者は守れないため、backend側の保護が必要。
4. mutable scanは初回resume時にexclusive lock下で対象範囲の整列と可視な`RecordView`の収集を行い、lockを解放してからyieldする。レコード本体は不変で、世代のarenaに保持する。これは大きい範囲ではメモリを使いwriterも待たせるが、初版の明確なsnapshot実装になる。
5. `freeze`では葉の遅延sortなどの状態も確定する。freeze後のiteratorが内部状態を書き換えないようにしてから、SST向けにストリーム走査する。
6. 有界保持と`retainAllVersions`、application順とsequence順、同一seqの再適用を既存契約に合わせる。
7. split時の葉・アンカー・metadata・hash拡張は公開前に準備し、失敗しても旧索引が引き続き検索できる構造にする。複数箇所のmetadata更新を途中で公開しない。
8. ハッシュ衝突は完全キー比較で確認し、overflow/chaining等で終了可能な経路を用意する。指紋一致だけで結果を決めず、解消しない拡張を無制限に繰り返さない。
9. ノードやアンカーは最初は世代の寿命まで保持して回収を単純化する。サイズ計上にはretired領域も含め、毎putの全体コピーやメモリの二重計上を避ける。

`entryCount`は一意なキー数ではなく、保持しているレコード版の数として計上する。
`flushIterator`も各キーの最新1版だけに縮めず、必要な全保持版を列挙する。

mutable scanで集めたviewが有効なのは、所有するMemTable世代が生きている間。
これは`MemTable::RangeIterator`側のsource保持と合わせて検証する。
有界版のリング自体をiteratorで参照し続けるだけでは、上書きが進んだときのsnapshotを保持できない。

## 進め方と実装難度

| 段階 | 完了させる内容 | 難度 |
|---|---|---|
| 単独の索引コア | LeafList、prefix lookup、不在キーのseek、split、binary境界 | 高 |
| 安全なMemTable版 | VersionChain、snapshot、freeze、flush全版、例外・計上 | 高 |
| 既存engineへの接続 | factory、CMake、明示的なbackend選択、共通テスト | 低〜中 |
| 性能検証 | 同じ契約で既存3方式と比較し、採用判断 | 中 |
| 並行性の高度化 | 葉単位lock、metadata公開、回収、iterator snapshotの改良 | 非常に高 |

単独コアの段階で多版契約が未実装なら、まだ本番用`IMemTable`として登録しない。
最初から細粒度RCU・ポインタ圧縮・SIMD tag最適化まで再現するより、終了性と失敗時の整合性を先に固定する。
安全版がARTより遅い場合も結果として受け入れ、測定で大きい費用が判明した部分を次に改善する。

## 必要な検証

- `std::map`等をoracleにして、ランダム操作と全順序・範囲結果を比較する。空キー、0x00/0xff、prefixとその延長、存在しない上下端、空範囲を含める。
- 葉容量の前後、長い共通prefix、metadataの人工的なhash衝突、拡張とsplitの割当て失敗を試す。失敗後にも旧キーが全て見つかり、操作が成功または失敗として終了し、論理カウンタが一致することを確認する。
- 更新が保持数を超えても、停止中のscanが捕捉した値を維持する。iteratorを完走・破棄・例外終了させた後に保護を解放する。
- sequence順とapplication順、遅れて到着したseq、同一seq再適用、tombstone、Blob参照フラグ、全履歴保持を確認する。
- freezeと読み取り、単一writerと複数reader、scan停止中の更新、forceFlushとの競合を試す。generatorのthreadを替える使い方を許す場合は、直列resumeでも安全なことを確認する。
- SSTには各キーの必要な保持版を全て渡す。flush後にstoreを閉じ、再open/recoverしてキー・seq・値・削除状態を検証する。
- 対応する環境でASan/UBSanとthread sanitizerを使い、通常の回帰テストと分けて検証する。

## 性能比較の条件

現在の10万キー・16バイトキーだけで採用を決めない。
長さ8/16/64/256/1024バイト、共通prefixの多寡、10万/100万以上のworking set、hit/miss、更新割合を分ける。
scanは1/32/128/1024行と全件を分け、葉の遅延sortがある場合は初回と整列済みを別に測る。

MVCC・値のコピー・snapshot保護・warmup・CPU affinity・同じ論理状態をそろえて既存3方式と比較する。
参照実装の単版インデックスだけの数字と、版管理付きNativeの数字を並べて勝敗を決めない。
今回の「短いキーで共通prefixが長い」入力では、既に速いARTを上回る保証はない。

SST込みでは、共通の圧縮・CRC・Manifest・同期も残る。
Wormholeはflushの列挙速度にも影響し得るが、追加だけで90万件/sになるとは判断できない。
固定件数で1ファイルへflushする測定と、同じメモリ閾値で投入から永続化完了までの測定を分ける。
長時間運転とcompaction込みの持続性能も、短いバッチとは別に確認する。

## 採用判断

安全な追加バックエンドとして実装を進める価値はある。
ただし、参照C実装の既知制約をそのまま許容する導入や、未検証の段階でART/SkipListを置き換える導入は推奨しない。
最初の判断材料は「多版・snapshot・異常系を満たしたNative版が、どのキー集合で既存方式より良いか」とする。
