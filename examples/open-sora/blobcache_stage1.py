_base_ = ["stage1.py"]

dataset = dict(
    data_path="/shared/blobcache-deploy/pexels_meta.csv",
    vmaf=False,
)

model = dict(from_pretrained="/blobcache/osv2/Open_Sora_v2.safetensors")
ae = dict(from_pretrained="/blobcache/osv2/hunyuan_vae.safetensors")
t5 = dict(
    from_pretrained="/blobcache/osv2/google/t5-v1_1-xxl",
    cache_dir="/tmp/hf",
)
clip = dict(
    from_pretrained="/blobcache/osv2/openai/clip-vit-large-patch14",
    cache_dir="/tmp/hf",
)

outputs = "/mnt/nvme/os-train-out"
epochs = 1
log_every = 1
ckpt_every = 100000
num_workers = 8
num_bucket_build_workers = 32
