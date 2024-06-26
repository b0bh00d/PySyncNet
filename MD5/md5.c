#include "md5.h"

// http://www.zedwood.com/article/cpp-md5-function
// https://github.com/CommanderBubble/MD5
// https://ofstack.com/C++/20865/c-and-c++-md5-algorithm-implementation-code.html

static void         _update(MD5_CTX* context, const unsigned char *input, size_t inputlen);
static PyObject*    _final(MD5_CTX* context);

static void         snmd5_capsule_destructor(PyObject* capsule);
static void         snmd5_init(MD5_CTX *context);
static void         snmd5_transform(unsigned int state[4], const unsigned char block[64]);
static void         snmd5_encode(unsigned char *output, const unsigned int *input, unsigned int len);
static void         snmd5_decode(unsigned int *output, const unsigned char *input, unsigned int len);

static unsigned char PADDING[] = {
  0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

static PyObject* snmd5_new(PyObject *self)
{
    MD5_CTX* context = (MD5_CTX*)malloc(sizeof(MD5_CTX));
    snmd5_init(context);

    return PyCapsule_New((void*)context, "context", snmd5_capsule_destructor);
}

static PyObject* snmd5_update(PyObject *self, PyObject* args)
{
    PyObject* capsule;
    const unsigned char* input = NULL;
    Py_ssize_t inputlen;

    if(PyArg_ParseTuple(args, "Oy#", &capsule, (const char *)&input, &inputlen))
    {
        if(PyCapsule_CheckExact(capsule))
        {
            MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
            _update(context, input, inputlen);
            Py_RETURN_TRUE;
        }
    }

    Py_RETURN_FALSE;
}

static void _update(MD5_CTX* context, const unsigned char *input, size_t inputlen)
{
    unsigned int i = 0, index = 0, partlen = 0;
    index = (context->count[0] >> 3) & 0x3F;
    partlen = 64 - index;
    context->count[0] += inputlen << 3;
    if(context->count[0] < (inputlen << 3))
        context->count[1]++;
    context->count[1] += inputlen >> 29;

    if(inputlen >= partlen)
    {
        memcpy(&context->buffer[index], input, partlen);
        snmd5_transform(context->state, context->buffer);
        for (i = partlen; i + 64 <= inputlen; i += 64)
            snmd5_transform(context->state, &input[i]);
        index = 0;
    }
    else
        i = 0;

    memcpy(&context->buffer[index], &input[i], inputlen - i);
}

static PyObject* _final(MD5_CTX* context)
{
    unsigned char digest[16];
    unsigned int index = 0, padlen = 0;
    unsigned char bits[8];

    index = (context->count[0] >> 3) & 0x3F;
    padlen = (index < 56) ? (56 - index) : (120 - index);
    snmd5_encode(bits, context->count, 8);
    _update(context, PADDING, padlen);
    _update(context, bits, 8);
    snmd5_encode(digest, context->state, 16);

    return Py_BuildValue("y#", digest, 16);
}

static PyObject* snmd5_final(PyObject *self, PyObject* args)
{
    PyObject* capsule;
    if(PyArg_ParseTuple(args, "O", &capsule))
    {
        if(PyCapsule_CheckExact(capsule))
        {
            MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
            if(context)
                return _final(context);
        }
    }

    Py_RETURN_NONE;
}

static PyObject* snmd5_snapshot(PyObject *self, PyObject* args)
{
    PyObject* capsule;
    if(PyArg_ParseTuple(args, "O", &capsule))
    {
        if(PyCapsule_CheckExact(capsule))
        {
            MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
            if(context)
            {
                MD5_CTX snapshot;
                memcpy((void*)&snapshot, (void*)context, sizeof(MD5_CTX));
                return _final(&snapshot);
            }
        }
    }

    Py_RETURN_NONE;
}

static PyObject* snmd5_getstate(PyObject* self, PyObject* args)
{
    PyObject* capsule;
    if(PyArg_ParseTuple(args, "O", &capsule))
    {
        if(PyCapsule_CheckExact(capsule))
        {
            MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
            if(context)
                return Py_BuildValue("y#", (const char *)context, sizeof(MD5_CTX));
        }
    }

    Py_RETURN_NONE;
}

static PyObject* snmd5_setstate(PyObject* self, PyObject* args)
{
    PyObject* capsule;
    const unsigned char* input = NULL;
    Py_ssize_t inputlen;

    if(PyArg_ParseTuple(args, "Oy#", &capsule, (const char *)&input, &inputlen))
    {
        if(PyCapsule_CheckExact(capsule))
        {
            MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
            if(context)
            {
                if(inputlen == sizeof(MD5_CTX))
                {
                    memcpy((char*)context, (const char *)input, sizeof(MD5_CTX));
                    Py_RETURN_TRUE;
                }
            }
        }
    }

    Py_RETURN_FALSE;
}

/* module-local function */
static void snmd5_capsule_destructor(PyObject* capsule)
{
    MD5_CTX* context = PyCapsule_GetPointer(capsule, "context");
    if(context)
        free(context);
}

/* module-local function */
static void snmd5_init(MD5_CTX *context)
{
    context->count[0] = 0;
    context->count[1] = 0;
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
}

/* module-local function */
static void snmd5_encode(unsigned char *output, const unsigned int *input, unsigned int len)
{
    unsigned int i = 0, j = 0;
    while (j < len)
    {
        output[j] = input[i] & 0xFF;
        output[j + 1] = (input[i] >> 8) & 0xFF;
        output[j + 2] = (input[i] >> 16) & 0xFF;
        output[j + 3] = (input[i] >> 24) & 0xFF;
        i++;
        j += 4;
    }
}

/* module-local function */
static void snmd5_decode(unsigned int *output, const unsigned char *input, unsigned int len)
{
    unsigned int i = 0, j = 0;
    while (j < len)
    {
        output[i] = (input[j]) |
        (input[j + 1] << 8) |
        (input[j + 2] << 16) |
        (input[j + 3] << 24);
        i++;
        j += 4;
    }
}

/* module-local function */
static void snmd5_transform(unsigned int state[4], const unsigned char block[64])
{
  unsigned int a = state[0];
  unsigned int b = state[1];
  unsigned int c = state[2];
  unsigned int d = state[3];
  unsigned int x[64];

  snmd5_decode(x, block, 64);
  FF(a, b, c, d, x[0], 7, 0xd76aa478);
  FF(d, a, b, c, x[1], 12, 0xe8c7b756);
  FF(c, d, a, b, x[2], 17, 0x242070db);
  FF(b, c, d, a, x[3], 22, 0xc1bdceee);
  FF(a, b, c, d, x[4], 7, 0xf57c0faf);
  FF(d, a, b, c, x[5], 12, 0x4787c62a);
  FF(c, d, a, b, x[6], 17, 0xa8304613);
  FF(b, c, d, a, x[7], 22, 0xfd469501);
  FF(a, b, c, d, x[8], 7, 0x698098d8);
  FF(d, a, b, c, x[9], 12, 0x8b44f7af);
  FF(c, d, a, b, x[10], 17, 0xffff5bb1);
  FF(b, c, d, a, x[11], 22, 0x895cd7be);
  FF(a, b, c, d, x[12], 7, 0x6b901122);
  FF(d, a, b, c, x[13], 12, 0xfd987193);
  FF(c, d, a, b, x[14], 17, 0xa679438e);
  FF(b, c, d, a, x[15], 22, 0x49b40821);


  GG(a, b, c, d, x[1], 5, 0xf61e2562);
  GG(d, a, b, c, x[6], 9, 0xc040b340);
  GG(c, d, a, b, x[11], 14, 0x265e5a51);
  GG(b, c, d, a, x[0], 20, 0xe9b6c7aa);
  GG(a, b, c, d, x[5], 5, 0xd62f105d);
  GG(d, a, b, c, x[10], 9, 0x2441453);
  GG(c, d, a, b, x[15], 14, 0xd8a1e681);
  GG(b, c, d, a, x[4], 20, 0xe7d3fbc8);
  GG(a, b, c, d, x[9], 5, 0x21e1cde6);
  GG(d, a, b, c, x[14], 9, 0xc33707d6);
  GG(c, d, a, b, x[3], 14, 0xf4d50d87);
  GG(b, c, d, a, x[8], 20, 0x455a14ed);
  GG(a, b, c, d, x[13], 5, 0xa9e3e905);
  GG(d, a, b, c, x[2], 9, 0xfcefa3f8);
  GG(c, d, a, b, x[7], 14, 0x676f02d9);
  GG(b, c, d, a, x[12], 20, 0x8d2a4c8a);


  HH(a, b, c, d, x[5], 4, 0xfffa3942);
  HH(d, a, b, c, x[8], 11, 0x8771f681);
  HH(c, d, a, b, x[11], 16, 0x6d9d6122);
  HH(b, c, d, a, x[14], 23, 0xfde5380c);
  HH(a, b, c, d, x[1], 4, 0xa4beea44);
  HH(d, a, b, c, x[4], 11, 0x4bdecfa9);
  HH(c, d, a, b, x[7], 16, 0xf6bb4b60);
  HH(b, c, d, a, x[10], 23, 0xbebfbc70);
  HH(a, b, c, d, x[13], 4, 0x289b7ec6);
  HH(d, a, b, c, x[0], 11, 0xeaa127fa);
  HH(c, d, a, b, x[3], 16, 0xd4ef3085);
  HH(b, c, d, a, x[6], 23, 0x4881d05);
  HH(a, b, c, d, x[9], 4, 0xd9d4d039);
  HH(d, a, b, c, x[12], 11, 0xe6db99e5);
  HH(c, d, a, b, x[15], 16, 0x1fa27cf8);
  HH(b, c, d, a, x[2], 23, 0xc4ac5665);


  II(a, b, c, d, x[0], 6, 0xf4292244);
  II(d, a, b, c, x[7], 10, 0x432aff97);
  II(c, d, a, b, x[14], 15, 0xab9423a7);
  II(b, c, d, a, x[5], 21, 0xfc93a039);
  II(a, b, c, d, x[12], 6, 0x655b59c3);
  II(d, a, b, c, x[3], 10, 0x8f0ccc92);
  II(c, d, a, b, x[10], 15, 0xffeff47d);
  II(b, c, d, a, x[1], 21, 0x85845dd1);
  II(a, b, c, d, x[8], 6, 0x6fa87e4f);
  II(d, a, b, c, x[15], 10, 0xfe2ce6e0);
  II(c, d, a, b, x[6], 15, 0xa3014314);
  II(b, c, d, a, x[13], 21, 0x4e0811a1);
  II(a, b, c, d, x[4], 6, 0xf7537e82);
  II(d, a, b, c, x[11], 10, 0xbd3af235);
  II(c, d, a, b, x[2], 15, 0x2ad7d2bb);
  II(b, c, d, a, x[9], 21, 0xeb86d391);

  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
}

static PyMethodDef snmd5_methods[] = {
    /* The cast of the function is necessary since PyCFunction values
     * only take two PyObject* parameters, and keywdarg_parrot() takes
     * three.
     */
    {"new", (PyCFunction)(void(*)(void))snmd5_new, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {"update", (PyCFunction)(void(*)(void))snmd5_update, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {"snapshot", (PyCFunction)(void(*)(void))snmd5_snapshot, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {"finalize", (PyCFunction)(void(*)(void))snmd5_final, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {"getState", (PyCFunction)(void(*)(void))snmd5_getstate, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {"setState", (PyCFunction)(void(*)(void))snmd5_setstate, METH_VARARGS | METH_KEYWORDS, "Print a lovely skit to standard output."},
    {NULL, NULL, 0, NULL}   /* sentinel */
};

// https://docs.python.org/3/c-api/module.html#c.PyModuleDef
static struct PyModuleDef snmd5_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "snmd5",
    .m_doc = NULL,
    .m_size = -1,
    .m_methods = snmd5_methods,
    .m_slots = NULL,
    .m_traverse = NULL,
    .m_clear = NULL,
    .m_free = NULL
};

PyMODINIT_FUNC
PyInit_snmd5(void)
{
    // PyObject* module;
    // PyObject* capsule;
    // MD5_CTX* context;

    // context = (MD5_CTX*)malloc(sizeof(MD5_CTX));
    // snmd5_init(context);

    // capsule = PyCapsule_New((void*)context, "context", capssule_destructor);

    // module = PyModule_Create(&snmd5_module);
    // if(PyModule_AddObject(module, "context", capsule) < 0)
    // {
    //     free(context);

    //     Py_XDECREF(capsule);
    //     Py_CLEAR(capsule);
    //     Py_DECREF(module);

    //     return NULL;
    // }

    return PyModule_Create(&snmd5_module);
}
